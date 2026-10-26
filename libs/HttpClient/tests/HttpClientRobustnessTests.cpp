// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/HttpClientImpl.hpp"

#include <gtest/gtest.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>
namespace AllocationFailure {
extern thread_local int allocationsUntilFailure;
extern thread_local std::size_t failureMinimumSize;
} // namespace AllocationFailure

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using Tcp = asio::ip::tcp;

constexpr const char* const OkResponse = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

using HttpClientTests::RunBounded;
using HttpClientTests::ScriptedServer;
using HttpClientTests::ServerAction;

AVEVA::HttpRequest MakeRequest(const std::string& url, AVEVA::HttpMethod method = AVEVA::HttpMethod::Get) {
    AVEVA::HttpRequest request;
    request.SetMethod(method);
    request.SetUrl(url);
    if (method == AVEVA::HttpMethod::Post) {
        request.SetBody("payload");
    }
    return request;
}

TEST(HttpClientRobustness, EofDelimitedResponseIsNotReturnedToThePool) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
        if (connection == 0) {
            // No Content-Length and no chunking: the body ends when the server closes the connection.
            return {"HTTP/1.1 200 OK\r\n\r\nbody", true};
        }
        return {OkResponse};
    });

    std::vector<std::error_code> errors;
    std::vector<std::string> bodies;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse response) {
                          errors.push_back(error);
                          bodies.push_back(response.GetBody());
                          // A POST is never silently re-sent, so it only succeeds if it gets a fresh connection.
                          client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                                            AVEVA::HttpRequestOptions{},
                                            [&](std::error_code secondError, AVEVA::HttpResponse secondResponse) {
                                                errors.push_back(secondError);
                                                bodies.push_back(secondResponse.GetBody());
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2u);
    EXPECT_FALSE(errors[0]);
    EXPECT_EQ(bodies[0], "body");
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(bodies[1], "OK");
    EXPECT_EQ(server.Accepted(), 2);
    EXPECT_EQ(server.TotalRequests(), 2);
}

TEST(HttpClientRobustness, ExtraBytesAfterACompleteResponseKeepTheConnectionOutOfThePool) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
        if (connection == 0) {
            return {"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOKX"};
        }
        return {OkResponse};
    });

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                                            AVEVA::HttpRequestOptions{},
                                            [&](std::error_code secondError, AVEVA::HttpResponse secondResponse) {
                                                errors.push_back(secondError);
                                                EXPECT_EQ(secondResponse.GetStatus(), 200U);
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_FALSE(errors[0]);
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(server.Accepted(), 2);
    EXPECT_EQ(server.TotalRequests(), 2);
}

TEST(HttpClientRobustness, IdempotentRequestIsRetriedOnAStalePooledConnection) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
        // The first connection is closed right after its keep-alive response, so the pooled stream is dead.
        return {OkResponse, connection == 0};
    });

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          // Give the server time to close its end before the pooled connection is reused.
                          auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(100));
                          timer->async_wait([&, timer](boost::system::error_code) {
                              client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                                [&](std::error_code secondError, AVEVA::HttpResponse secondResponse) {
                                                    errors.push_back(secondError);
                                                    EXPECT_EQ(secondResponse.GetStatus(), 200u);
                                                    server.Stop();
                                                });
                          });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2u);
    EXPECT_FALSE(errors[0]);
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(server.Accepted(), 2);
}

TEST(HttpClientRobustness, PostLostOnAReusedConnectionIsNotSentAgain) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        ServerAction action{OkResponse};
        // The first connection receives the POST and then dies without answering it.
        action.Drop = connection == 0 && request.method() == http::verb::post;
        return action;
    });

    std::error_code postError;
    client->SendAsync(
        MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
            client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  postError = error;
                                  // Let a (wrong) resend arrive on a new connection before the server is stopped.
                                  auto settle =
                                      std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(200));
                                  settle->async_wait([&, settle](boost::system::error_code) { server.Stop(); });
                              });
        });
    RunBounded(context);

    EXPECT_TRUE(postError);
    EXPECT_EQ(server.Requests(http::verb::post), 1);
}

// A PUT or DELETE that reached the server may already have taken effect; replaying it here would hide that
// from the Azure retry layer, which alone knows whether the request is safe to resend.
void ExpectMutationLostOnAReusedConnectionIsNotSentAgain(AVEVA::HttpMethod method, http::verb verb) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context,
                          [verb](int connection, const http::request<http::string_body>& request) -> ServerAction {
                              ServerAction action{OkResponse};
                              action.Drop = connection == 0 && request.method() == verb;
                              return action;
                          });

    std::error_code mutationError;
    client->SendAsync(
        MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
            client->SendAsync(MakeRequest(server.Url("/second"), method), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  mutationError = error;
                                  auto settle =
                                      std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(200));
                                  settle->async_wait([&, settle](boost::system::error_code) { server.Stop(); });
                              });
        });
    RunBounded(context);

    EXPECT_TRUE(mutationError);
    EXPECT_EQ(server.Requests(verb), 1);
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, DeleteLostOnAReusedConnectionIsNotSentAgain) {
    ExpectMutationLostOnAReusedConnectionIsNotSentAgain(AVEVA::HttpMethod::Delete, http::verb::delete_);
}

TEST(HttpClientRobustness, PutLostOnAReusedConnectionIsNotSentAgain) {
    ExpectMutationLostOnAReusedConnectionIsNotSentAgain(AVEVA::HttpMethod::Put, http::verb::put);
}

TEST(HttpClientRobustness, GetLostOnAReusedConnectionIsRetried) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        ServerAction action{OkResponse};
        action.Drop = connection == 0 && request.target() == "/second";
        return action;
    });

    std::error_code secondError;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code, AVEVA::HttpResponse) {
                          client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                            [&](std::error_code error, AVEVA::HttpResponse) {
                                                secondError = error;
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    EXPECT_FALSE(secondError);
    EXPECT_EQ(server.Accepted(), 2);
}

TEST(HttpClientRobustness, GetIsNotRetriedOnceResponseBytesWereReceivedOnAReusedConnection) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        if (connection == 0 && request.target() == "/second") {
            // The server accepted the request and started answering before dying.
            return {"HTTP/1.1 200 OK\r\nContent-Le", true};
        }
        return {OkResponse};
    });

    std::error_code secondError;
    client->SendAsync(
        MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
            client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  secondError = error;
                                  auto settle =
                                      std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(200));
                                  settle->async_wait([&, settle](boost::system::error_code) { server.Stop(); });
                              });
        });
    RunBounded(context);

    EXPECT_TRUE(secondError);
    EXPECT_EQ(server.Accepted(), 1);
    EXPECT_EQ(server.TotalRequests(), 2);
}

TEST(HttpClientRobustness, StalledServerCompletesOnceWithTimeout) {
    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds(100));
    auto result = HttpClientTests::Exchange({}, {}, requestOptions, true);

    EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
}

TEST(HttpClientRobustness, PlainHttpRequestTimesOut) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {.Hold = true}; });

    AVEVA::HttpRequestOptions options;
    options.SetTimeout(std::chrono::milliseconds(100));

    std::error_code observed;
    client->SendAsync(MakeRequest(server.Url("/timeout")), options, [&](std::error_code error, AVEVA::HttpResponse) {
        observed = error;
        server.Stop();
    });
    RunBounded(context);

    EXPECT_EQ(observed, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, TimeoutOfAStalledRawViewUploadCompletesOnceAndLetsTheCallerFreeTheBody) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    // Accepts the connection but never reads, so the client's upload stalls once the socket buffers fill.
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    Tcp::socket accepted(context);
    acceptor.async_accept(accepted, [](boost::system::error_code) {});

    auto body = std::make_unique<std::vector<std::byte>>(64U * 1024U * 1024U, std::byte{0x41});
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Put);
    request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
    request.SetBodyView(std::span<const std::byte>(*body));
    AVEVA::HttpRequestOptions options;
    options.SetTimeout(std::chrono::milliseconds(300));

    int completions = 0;
    std::error_code observed;
    client->SendAsync(std::move(request), options, [&](std::error_code error, AVEVA::HttpResponse) {
        ++completions;
        observed = error;
        body.reset(); // A late transport access would now touch freed memory.
        acceptor.close();
        accepted.close();
    });
    RunBounded(context);

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(observed, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
}

TEST(HttpClientRobustness, CancellingAStalledRawViewUploadCompletesOnceAndLetsTheCallerFreeTheBody) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    Tcp::socket accepted(context);
    acceptor.async_accept(accepted, [](boost::system::error_code) {});

    auto body = std::make_unique<std::vector<std::byte>>(64U * 1024U * 1024U, std::byte{0x41});
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Put);
    request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
    request.SetBodyView(std::span<const std::byte>(*body));
    asio::cancellation_signal cancellation;
    AVEVA::HttpRequestOptions options;
    options.SetCancellationSlot(cancellation.slot());

    int completions = 0;
    std::error_code observed;
    client->SendAsync(std::move(request), options, [&](std::error_code error, AVEVA::HttpResponse) {
        ++completions;
        observed = error;
        body.reset();
        acceptor.close();
        accepted.close();
    });
    asio::steady_timer cancelTimer(context, std::chrono::milliseconds(300));
    cancelTimer.async_wait([&](boost::system::error_code) { cancellation.emit(asio::cancellation_type::terminal); });
    RunBounded(context);

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(observed, std::make_error_code(std::errc::operation_canceled));
}

TEST(HttpClientRobustness, ReusedConnectionRequestTimesOut) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        if (connection == 0 && request.target() == "/second") {
            return {.Hold = true};
        }
        return {OkResponse};
    });

    AVEVA::HttpRequestOptions timeoutOptions;
    timeoutOptions.SetTimeout(std::chrono::milliseconds(100));

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          client->SendAsync(MakeRequest(server.Url("/second")), timeoutOptions,
                                            [&](std::error_code secondError, AVEVA::HttpResponse) {
                                                errors.push_back(secondError);
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_FALSE(errors[0]);
    EXPECT_EQ(errors[1], AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, MaximumRequestTimeoutDoesNotExpireImmediately) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(50)};
    });

    AVEVA::HttpRequestOptions options;
    options.SetTimeout(std::chrono::milliseconds::max());

    std::error_code observed;
    unsigned int status = 0;
    client->SendAsync(MakeRequest(server.Url("/max-timeout")), options,
                      [&](std::error_code error, AVEVA::HttpResponse response) {
                          observed = error;
                          status = response.GetStatus();
                          server.Stop();
                      });
    RunBounded(context);

    EXPECT_FALSE(observed);
    EXPECT_EQ(status, 200U);
}

TEST(HttpClientRobustness, IdleTimeoutExpiryClosesThePooledConnection) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetIdleConnectionTimeout(std::chrono::seconds(1));
    auto client = AVEVA::HttpClient::Create(context, options);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(1200));
                          timer->async_wait([&, timer](boost::system::error_code waitError) {
                              ASSERT_FALSE(waitError);
                              client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                                [&](std::error_code secondError, AVEVA::HttpResponse) {
                                                    errors.push_back(secondError);
                                                    server.Stop();
                                                });
                          });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_FALSE(errors[0]);
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(server.Accepted(), 2);
}

TEST(HttpClientRobustness, MaxConnectionsPerHostQueuesExtraRequestsAndReusesTheConnection) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetMaxConnectionsPerHost(1);
    auto client = AVEVA::HttpClient::Create(context, options);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });

    std::vector<std::error_code> errors;
    for (int i = 0; i < 4; ++i) {
        client->SendAsync(MakeRequest(server.Url("/" + std::to_string(i))), AVEVA::HttpRequestOptions{},
                          [&](std::error_code error, AVEVA::HttpResponse) {
                              errors.push_back(error);
                              if (errors.size() == 4) {
                                  server.Stop();
                              }
                          });
    }
    RunBounded(context);

    ASSERT_EQ(errors.size(), 4U);
    for (const auto& error : errors) {
        EXPECT_FALSE(error);
    }
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, QueuedRequestStartsWhileThePreviousHandlerIsStillRunning) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetMaxConnectionsPerHost(1);
    // A pooled connection would put the queued request on the blocked handler's strand.
    options.SetMaxIdleConnectionsPerHost(0);
    auto client = AVEVA::HttpClient::Create(context, options);
    // ScriptedServer is not thread-safe, so it gets its own single-threaded context.
    asio::io_context serverContext;
    ScriptedServer server(serverContext,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });
    auto serverGuard = asio::make_work_guard(serverContext);
    std::thread serverThread([&] { serverContext.run(); });

    std::promise<void> secondDone;
    auto secondFuture = secondDone.get_future();
    std::atomic<bool> startedWhileBlocked = false;
    std::atomic<int> completed = 0;
    auto guard = asio::make_work_guard(context);
    client->SendAsync(
        MakeRequest(server.Url("/a")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
            // The slot must already be free here, so the queued request can finish on another thread
            // while this handler is still blocked.
            startedWhileBlocked = secondFuture.wait_for(std::chrono::seconds(4)) == std::future_status::ready;
            ++completed;
            guard.reset();
        });
    client->SendAsync(MakeRequest(server.Url("/b")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code, AVEVA::HttpResponse) {
                          secondDone.set_value();
                          asio::post(serverContext, [&] { server.Stop(); });
                      });
    std::thread helper([&context] { context.run_for(std::chrono::seconds(8)); });
    context.run_for(std::chrono::seconds(8));
    helper.join();
    serverGuard.reset();
    serverThread.join();

    EXPECT_TRUE(startedWhileBlocked);
}

TEST(HttpClientRobustness, TimeSpentQueuedForASlotCountsAgainstTheRequestTimeout) {
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    auto client = AVEVA::HttpClient::Create(context, clientOptions);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {.Hold = true}; });

    AVEVA::HttpRequestOptions holdingOptions;
    holdingOptions.SetTimeout(std::chrono::milliseconds(400));
    AVEVA::HttpRequestOptions queuedOptions;
    queuedOptions.SetTimeout(std::chrono::milliseconds(150));

    std::error_code firstResult;
    std::error_code queuedResult;
    std::chrono::steady_clock::duration queuedElapsed{};
    const auto submitted = std::chrono::steady_clock::now();
    int completions = 0;
    const auto finishWhenBothDone = [&] {
        if (++completions == 2) {
            server.Stop();
        }
    };
    client->SendAsync(MakeRequest(server.Url("/held")), holdingOptions,
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          firstResult = error;
                          finishWhenBothDone();
                      });
    client->SendAsync(MakeRequest(server.Url("/queued")), queuedOptions,
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          queuedResult = error;
                          queuedElapsed = std::chrono::steady_clock::now() - submitted;
                          finishWhenBothDone();
                      });
    RunBounded(context);

    EXPECT_EQ(firstResult, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    EXPECT_EQ(queuedResult, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    // It is woken at its own 150 ms deadline, not when the first request frees the slot at 400 ms.
    EXPECT_LT(queuedElapsed, std::chrono::milliseconds(390));
    // The queued request's deadline passed while it waited, so it never opened a connection of its own.
    EXPECT_EQ(server.Accepted(), 1);
    EXPECT_EQ(server.TotalRequests(), 1);
}

TEST(HttpClientRobustness, CancellingAQueuedRequestFailsItWithoutStartingIt) {
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    auto client = AVEVA::HttpClient::Create(context, clientOptions);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Hold = true;
        return action;
    });

    asio::cancellation_signal firstCancel;
    AVEVA::HttpRequestOptions firstOptions;
    firstOptions.SetCancellationSlot(firstCancel.slot());
    asio::cancellation_signal queuedCancel;
    AVEVA::HttpRequestOptions queuedOptions;
    queuedOptions.SetCancellationSlot(queuedCancel.slot());

    std::error_code firstResult;
    std::error_code queuedResult;
    std::chrono::steady_clock::duration queuedElapsed{};
    const auto submitted = std::chrono::steady_clock::now();
    int completions = 0;
    const auto finishWhenBothDone = [&] {
        if (++completions == 2) {
            server.Stop();
        }
    };
    client->SendAsync(MakeRequest(server.Url("/held")), firstOptions, [&](std::error_code error, AVEVA::HttpResponse) {
        firstResult = error;
        finishWhenBothDone();
    });
    client->SendAsync(MakeRequest(server.Url("/queued")), queuedOptions,
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          queuedResult = error;
                          // The queued request is cancelled first; the held one is released only after that completion.
                          firstCancel.emit(asio::cancellation_type::terminal);
                          finishWhenBothDone();
                      });
    queuedCancel.emit(asio::cancellation_type::terminal);
    RunBounded(context);

    EXPECT_EQ(queuedResult, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(firstResult, std::make_error_code(std::errc::operation_canceled));
    EXPECT_LE(server.Accepted(), 1);
}

TEST(HttpClientRobustness, MaximumIdleTimeoutStillAllowsPooling) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetIdleConnectionTimeout(std::chrono::seconds::max());
    auto client = AVEVA::HttpClient::Create(context, options);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                            [&](std::error_code secondError, AVEVA::HttpResponse) {
                                                errors.push_back(secondError);
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_FALSE(errors[0]);
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, CompletionRunsWhenTheHandlerExecutorHasNoOtherWork) {
    asio::io_context clientContext;
    asio::io_context handlerContext;
    auto client = AVEVA::HttpClient::Create(clientContext);
    // ScriptedServer is not thread-safe, so it gets its own single-threaded context.
    asio::io_context serverContext;
    ScriptedServer server(serverContext, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action{OkResponse};
        action.Delay = std::chrono::milliseconds(150);
        return action;
    });
    auto serverGuard = asio::make_work_guard(serverContext);
    std::thread serverThread([&] { serverContext.run(); });

    std::atomic_bool completed = false;
    std::error_code result;
    client->SendAsync(
        MakeRequest(server.Url()), AVEVA::HttpRequestOptions{},
        asio::bind_executor(handlerContext.get_executor(), [&](std::error_code error, AVEVA::HttpResponse) {
            result = error;
            completed = true;
            asio::post(serverContext, [&] { server.Stop(); });
            serverGuard.reset();
        }));

    std::thread clientThread([&] { clientContext.run(); });
    // Without outstanding work on the handler's executor this would return immediately.
    handlerContext.run();
    clientThread.join();
    serverThread.join();

    EXPECT_TRUE(completed);
    EXPECT_FALSE(result);
}

TEST(HttpClientRobustness, CancellationAtVaryingStagesCompletesExactlyOnce) {
    for (int iteration = 0; iteration < 25; ++iteration) {
        asio::io_context context;
        auto client = AVEVA::HttpClient::Create(context);
        ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
            ServerAction action;
            action.Hold = true;
            return action;
        });

        asio::cancellation_signal cancellation;
        AVEVA::HttpRequestOptions options;
        options.SetCancellationSlot(cancellation.slot());
        options.SetTimeout(std::chrono::seconds(10));

        int completions = 0;
        std::error_code result;
        client->SendAsync(MakeRequest(server.Url()), options, [&](std::error_code error, AVEVA::HttpResponse) {
            ++completions;
            result = error;
            server.Stop();
        });

        // Emit from the context rather than another thread: a slot's handler may only be reached by whoever
        // owns the slot, and re-posting a varying number of times lands the cancel at different stages.
        std::function<void(int)> emitAfter = [&](int hops) {
            if (hops == 0) {
                cancellation.emit(asio::cancellation_type::terminal);
                return;
            }
            asio::post(context, [&, hops] { emitAfter(hops - 1); });
        };
        asio::post(context, [&] { emitAfter(iteration % 5 * 3); });
        RunBounded(context);

        EXPECT_EQ(completions, 1);
        EXPECT_EQ(result, std::make_error_code(std::errc::operation_canceled));
    }
}

TEST(HttpClientRobustness, CancellationDuringStaleConnectionRetryCompletesExactlyOnce) {
    for (int iteration = 0; iteration < 25; ++iteration) {
        asio::io_context context;
        auto client = AVEVA::HttpClient::Create(context);
        ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
            return {OkResponse, connection == 0};
        });

        asio::cancellation_signal cancellation;
        int completions = 0;
        std::error_code secondResult;
        client->SendAsync(
            MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
                auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(50));
                timer->async_wait([&, timer](boost::system::error_code) {
                    AVEVA::HttpRequestOptions options;
                    options.SetCancellationSlot(cancellation.slot());
                    client->SendAsync(MakeRequest(server.Url("/second")), options,
                                      [&](std::error_code error, AVEVA::HttpResponse) {
                                          ++completions;
                                          secondResult = error;
                                          server.Stop();
                                      });
                    // Lands while the reuse failure and the replacement connection are being processed.
                    auto cancelTimer =
                        std::make_shared<asio::steady_timer>(context, std::chrono::microseconds(iteration * 200));
                    cancelTimer->async_wait([&, cancelTimer](boost::system::error_code) {
                        cancellation.emit(asio::cancellation_type::terminal);
                    });
                });
            });
        RunBounded(context);

        EXPECT_EQ(completions, 1);
        if (secondResult) {
            EXPECT_EQ(secondResult, std::make_error_code(std::errc::operation_canceled));
        }
    }
}
std::error_code SendOnce(const std::string& wire, AVEVA::HttpRequestOptions options = {}) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context,
                          [&](int, const http::request<http::string_body>&) -> ServerAction { return {wire, true}; });
    std::error_code result;
    client->SendAsync(MakeRequest(server.Url()), options, [&](std::error_code error, AVEVA::HttpResponse) {
        result = error;
        server.Stop();
    });
    RunBounded(context);
    return result;
}

TEST(HttpClientRobustness, MalformedStatusLineIsAProtocolError) {
    EXPECT_EQ(SendOnce("NOT-HTTP garbage\r\n\r\n"), AVEVA::make_error_code(AVEVA::HttpClientError::ProtocolError));
}

TEST(HttpClientRobustness, MalformedChunkFramingIsAProtocolError) {
    EXPECT_EQ(SendOnce("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nhello\r\n0\r\n\r\n"),
              AVEVA::make_error_code(AVEVA::HttpClientError::ProtocolError));
}

TEST(HttpClientRobustness, DestroyingTheClientWithPooledIdleConnectionsClosesThem) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });

    std::error_code result;
    client->SendAsync(MakeRequest(server.Url("/")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          result = error;
                          // The connection is now idle in the pool; tear the client down with it inside.
                          client.reset();
                          auto settle = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(100));
                          settle->async_wait([&, settle](boost::system::error_code) { server.Stop(); });
                      });
    RunBounded(context);

    EXPECT_FALSE(result);
    EXPECT_EQ(client, nullptr);
}

TEST(HttpClientRobustness, DestroyingTheClientWhileLeadingASharedLookupCompletesTheRequestOnce) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    int completions = 0;
    for (int i = 0; i < 2; ++i) {
        client->SendAsync(MakeRequest("http://shared-lookup.invalid/" + std::to_string(i)), AVEVA::HttpRequestOptions{},
                          [&](std::error_code error, AVEVA::HttpResponse) {
                              EXPECT_TRUE(error);
                              ++completions;
                          });
    }
    client.reset();
    RunBounded(context);

    EXPECT_EQ(completions, 2);
}

TEST(HttpClientRobustness, StartedDnsLeaderKeepsItsPendingEntryWhileTheLookupIsInFlight) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    client->SendAsync(MakeRequest("http://leader-guard.invalid/"), AVEVA::HttpRequestOptions{},
                      [](std::error_code, AVEVA::HttpResponse) {});

    bool probedAsLeader = true;
    // Queued behind the request's start, before the resolver thread can post its completion.
    asio::post(context, [&] {
        probedAsLeader = AVEVA::Private::DnsCache::Shared().JoinOrLead(
            "leader-guard.invalid", "80",
            [] {
                return AVEVA::Private::ResolveWaiter(
                    std::shared_ptr<AVEVA::Private::RequestOperation<AVEVA::Private::PlainStream>>{});
            },
            &context);
        if (probedAsLeader) {
            AVEVA::Private::DnsCache::Shared().Finish("leader-guard.invalid", "80", asio::error::operation_aborted, {},
                                                      &context);
        }
    });
    RunBounded(context);

    EXPECT_FALSE(probedAsLeader);
}

TEST(HttpClientRobustness, OversizedBodyIsReportedAsResponseTooLarge) {
    AVEVA::HttpRequestOptions options;
    options.SetResponseBodyLimit(4);
    EXPECT_EQ(SendOnce("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789", options),
              AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
}

TEST(HttpClientRobustness, TruncatedMessageIsAReadFailureNotAProtocolError) {
    EXPECT_EQ(SendOnce("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nOK"),
              AVEVA::make_error_code(AVEVA::HttpClientError::ReadFailed));
}
TEST(HttpClientRobustness, RequestTargetIsBuiltFromPathAndQuery) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    std::vector<std::string> targets;
    ScriptedServer server(context, [&](int, const http::request<http::string_body>& request) -> ServerAction {
        targets.emplace_back(request.target());
        return {OkResponse};
    });
    const auto port = server.Url("").substr(std::string("http://127.0.0.1:").size());
    const std::vector<std::string> paths{"", "?a=1", "/p/q%20r?x=%2F&y=", "/p?"};
    std::size_t next = 0;
    std::function<void()> sendNext = [&] {
        if (next == paths.size()) {
            server.Stop();
            return;
        }
        client->SendAsync(MakeRequest("http://127.0.0.1:" + port + paths[next++]), AVEVA::HttpRequestOptions{},
                          [&](std::error_code error, AVEVA::HttpResponse) {
                              EXPECT_FALSE(error);
                              sendNext();
                          });
    };
    sendNext();
    RunBounded(context);

    EXPECT_EQ(targets, (std::vector<std::string>{"/", "/?a=1", "/p/q%20r?x=%2F&y=", "/p?"}));
}

struct ThrowBadAlloc : AVEVA::Private::NoFault {
    static void OnSubmit() { throw std::bad_alloc(); }
    static void OnStartLimited() noexcept {}
    static void OnFailPost() noexcept {}
};

// Throws from the Nth call to StartLimited, counted across the process; each test resets the counters.
struct ThrowOnStartLimited : AVEVA::Private::NoFault {
    static inline std::atomic<int> Calls{0};
    static inline std::atomic<int> ThrowOnCall{0};
    static void OnSubmit() noexcept {}
    static void OnStartLimited() {
        if (++Calls == ThrowOnCall) {
            throw std::bad_alloc();
        }
    }
    static void OnFailPost() noexcept {}
};

struct ThrowOnFailPost : AVEVA::Private::NoFault {
    static void OnSubmit() noexcept {}
    static void OnStartLimited() noexcept {}
    static void OnFailPost() { throw std::bad_alloc(); }
};

TEST(HttpClientRobustness, LimiterAbortWhoseFailurePostThrowsStillCompletesTheHandlerOnce) {
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    AVEVA::Private::BasicHttpClientImpl<ThrowOnFailPost> impl(context, clientOptions);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(400)};
    });

    std::vector<int> completions(2, 0);
    std::vector<std::error_code> results(2);
    AVEVA::HttpRequestOptions queuedOptions;
    queuedOptions.SetTimeout(std::chrono::milliseconds(100));
    // The first request holds the only slot, so the second expires in the queue and is aborted by the limiter.
    impl.Submit(MakeRequest(server.Url("/0")), AVEVA::HttpRequestOptions{},
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions[0];
                    results[0] = error;
                    server.Stop();
                }));
    impl.Submit(MakeRequest(server.Url("/1")), queuedOptions,
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions[1];
                    results[1] = error;
                }));
    EXPECT_NO_THROW(context.run());

    EXPECT_EQ(completions, (std::vector<int>{1, 1}));
    EXPECT_EQ(results[1], AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
}

TEST(HttpClientRobustness, ThrowingFirstStartReturnsTheOriginSlotAndFailsTheRequestOnce) {
    ThrowOnStartLimited::Calls = 0;
    ThrowOnStartLimited::ThrowOnCall = 1;
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    AVEVA::Private::BasicHttpClientImpl<ThrowOnStartLimited> impl(context, clientOptions);
    ScriptedServer server(
        context, [](int, const http::request<http::string_body>&) -> ServerAction { return {.Wire = OkResponse}; });

    std::vector<int> completions(2, 0);
    std::vector<std::error_code> results(2);
    impl.Submit(MakeRequest(server.Url("/0")), AVEVA::HttpRequestOptions{},
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions[0];
                    results[0] = error;
                    // With a cap of 1 this only starts if the failed request gave its slot back.
                    impl.Submit(MakeRequest(server.Url("/1")), AVEVA::HttpRequestOptions{},
                                AVEVA::Detail::MakeCompletion([&](std::error_code second, AVEVA::HttpResponse) {
                                    ++completions[1];
                                    results[1] = second;
                                    server.Stop();
                                }));
                }));
    EXPECT_NO_THROW(context.run());

    EXPECT_EQ(completions, (std::vector<int>{1, 1}));
    EXPECT_EQ(results[0], std::make_error_code(std::errc::not_enough_memory));
    EXPECT_FALSE(results[1]);
}

TEST(HttpClientRobustness, ThrowingReleaseTriggeredStartCompletesBothHandlersOnceAndKeepsTheOriginUsable) {
    ThrowOnStartLimited::Calls = 0;
    ThrowOnStartLimited::ThrowOnCall = 2;
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    AVEVA::Private::BasicHttpClientImpl<ThrowOnStartLimited> impl(context, clientOptions);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(50)};
    });

    std::vector<int> completions(3, 0);
    std::vector<std::error_code> results(3);
    const auto submit = [&](const int index, std::function<void()> then = {}) {
        impl.Submit(MakeRequest(server.Url("/" + std::to_string(index))), AVEVA::HttpRequestOptions{},
                    AVEVA::Detail::MakeCompletion([&, index, then](std::error_code error, AVEVA::HttpResponse) {
                        ++completions[index];
                        results[index] = error;
                        if (then) {
                            then();
                        }
                    }));
    };
    // The first request holds the only slot; the second waits and is started by the first one's release, which
    // is the start that throws. A third request afterwards proves the slot was not lost.
    submit(0);
    submit(1, [&] { submit(2, [&] { server.Stop(); }); });
    EXPECT_NO_THROW(context.run());

    EXPECT_EQ(completions, (std::vector<int>{1, 1, 1}));
    EXPECT_FALSE(results[0]);
    EXPECT_EQ(results[1], std::make_error_code(std::errc::not_enough_memory));
    EXPECT_FALSE(results[2]);
}

TEST(HttpClientRobustness, ThrowingInitiationCompletesTheHandlerExactlyOnceAndNeverInline) {
    asio::io_context context;
    AVEVA::Private::BasicHttpClientImpl<ThrowBadAlloc> impl(context, AVEVA::HttpClientOptions{});

    int completions = 0;
    std::error_code result;
    bool inSubmit = true;
    impl.Submit(MakeRequest(HttpClientTests::ClosedLocalUrl()), AVEVA::HttpRequestOptions{},
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions;
                    result = error;
                    EXPECT_FALSE(inSubmit);
                }));
    inSubmit = false;
    EXPECT_EQ(completions, 0);
    RunBounded(context);

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(result, std::make_error_code(std::errc::not_enough_memory));
}

struct ThrowOnAdmit : AVEVA::Private::NoFault {
    static inline std::atomic<int> Calls{0};
    static inline std::atomic<int> ThrowOnCall{0};
    static void OnAdmit() {
        if (++Calls == ThrowOnCall) {
            throw std::bad_alloc();
        }
    }
};

struct ThrowOnStateAlloc : AVEVA::Private::NoFault {
    static inline std::atomic<int> Calls{0};
    static void OnStateAlloc() {
        if (++Calls == 1) {
            throw std::bad_alloc();
        }
    }
};

// Admission throws after the request was moved into the limiter's hands; the failure must still reach the handler
// through the retained state, once, with the slot intact for whoever comes next.
static void RunFailedAdmission(const int failingAdmission, const bool withCancellation) {
    ThrowOnAdmit::Calls = 0;
    ThrowOnAdmit::ThrowOnCall = failingAdmission;
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    AVEVA::Private::BasicHttpClientImpl<ThrowOnAdmit> impl(context, clientOptions);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(50)};
    });

    std::vector<int> completions(3, 0);
    std::vector<std::error_code> results(3);
    std::vector<asio::cancellation_signal> signals(3);
    const auto submit = [&](const int index, std::function<void()> then = {}) {
        AVEVA::HttpRequestOptions options;
        if (withCancellation) {
            options.SetCancellationSlot(signals[index].slot());
        }
        impl.Submit(MakeRequest(server.Url("/" + std::to_string(index))), options,
                    AVEVA::Detail::MakeCompletion([&, index, then](std::error_code error, AVEVA::HttpResponse) {
                        ++completions[index];
                        results[index] = error;
                        if (then) {
                            then();
                        }
                    }));
    };
    // With admission 1 failing, request 0 never runs. With admission 2 failing, request 1 is the queued one,
    // behind request 0, which holds the only slot.
    submit(0, [&] {
        if (failingAdmission == 1) {
            submit(2, [&] { server.Stop(); });
        }
    });
    if (failingAdmission == 2) {
        submit(1, [&] { submit(2, [&] { server.Stop(); }); });
    } else {
        submit(1);
    }
    if (withCancellation) {
        signals[failingAdmission - 1].emit(asio::cancellation_type::terminal);
    }
    EXPECT_NO_THROW(context.run());

    const auto failed = static_cast<std::size_t>(failingAdmission - 1);
    EXPECT_EQ(completions[failed], 1);
    EXPECT_EQ(results[failed], std::make_error_code(std::errc::not_enough_memory));
    EXPECT_EQ(completions[2], 1);
    EXPECT_FALSE(results[2]);
}

TEST(HttpClientRobustness, FirstAdmissionFailureCompletesOnceAndLaterRequestsRun) { RunFailedAdmission(1, false); }

TEST(HttpClientRobustness, FirstAdmissionFailureWithCancellationCompletesOnce) { RunFailedAdmission(1, true); }

TEST(HttpClientRobustness, QueuedAdmissionFailureCompletesOnceAndLaterRequestsRun) { RunFailedAdmission(2, false); }

TEST(HttpClientRobustness, QueuedAdmissionFailureWithCancellationCompletesOnce) { RunFailedAdmission(2, true); }

TEST(HttpClientRobustness, FailedStateAllocationStillCompletesTheOriginalHandlerOnceAndNeverInline) {
    ThrowOnStateAlloc::Calls = 0;
    asio::io_context context;
    AVEVA::Private::BasicHttpClientImpl<ThrowOnStateAlloc> impl(context, AVEVA::HttpClientOptions{});

    int completions = 0;
    std::error_code result;
    bool inSubmit = true;
    impl.Submit(MakeRequest(HttpClientTests::ClosedLocalUrl()), AVEVA::HttpRequestOptions{},
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions;
                    result = error;
                    EXPECT_FALSE(inSubmit);
                }));
    inSubmit = false;
    EXPECT_EQ(completions, 0);
    RunBounded(context);

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(result, std::make_error_code(std::errc::not_enough_memory));
}

// The 16 KiB read buffer is the only allocation of that size a plain request makes, so failing it reaches the
// read-stage setup (which runs from a transport completion, not from submission) deterministically.
TEST(HttpClientRobustness, ReadBufferAllocationFailureCompletesOnceWithoutEscapingRunAndKeepsTheClientUsable) {
    asio::io_context context;
    AVEVA::Private::BasicHttpClientImpl<> impl(context, AVEVA::HttpClientOptions{});
    ScriptedServer server(
        context, [](int, const http::request<http::string_body>&) -> ServerAction { return {.Wire = OkResponse}; });

    std::vector<int> completions(2, 0);
    std::vector<std::error_code> results(2);
    impl.Submit(MakeRequest(server.Url("/0")), AVEVA::HttpRequestOptions{},
                AVEVA::Detail::MakeCompletion([&](std::error_code error, AVEVA::HttpResponse) {
                    ++completions[0];
                    results[0] = error;
                    AllocationFailure::allocationsUntilFailure = -1;
                    AllocationFailure::failureMinimumSize = 0;
                    impl.Submit(MakeRequest(server.Url("/1")), AVEVA::HttpRequestOptions{},
                                AVEVA::Detail::MakeCompletion([&](std::error_code second, AVEVA::HttpResponse) {
                                    ++completions[1];
                                    results[1] = second;
                                    server.Stop();
                                }));
                }));
    AllocationFailure::failureMinimumSize = 16 * 1024;
    AllocationFailure::allocationsUntilFailure = 0;
    EXPECT_NO_THROW(RunBounded(context));
    AllocationFailure::allocationsUntilFailure = -1;
    AllocationFailure::failureMinimumSize = 0;

    EXPECT_EQ(completions, (std::vector<int>{1, 1}));
    EXPECT_EQ(results[0], std::make_error_code(std::errc::not_enough_memory));
    EXPECT_FALSE(results[1]);
}

TEST(HttpClientRobustness, DestroyingTheClientFailsQueuedRequestsAndCompletesActiveOnesOnce) {
    asio::io_context context;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxConnectionsPerHost(1);
    auto client = AVEVA::HttpClient::Create(context, clientOptions);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Hold = true;
        return action;
    });

    std::vector<int> completions(3, 0);
    std::vector<std::error_code> results(3);
    int queuedDone = 0;
    for (int i = 0; i < 3; ++i) {
        AVEVA::HttpRequestOptions options;
        options.SetTimeout(std::chrono::seconds(10));
        client->SendAsync(MakeRequest(server.Url("/" + std::to_string(i))), options,
                          [&, i](std::error_code error, AVEVA::HttpResponse) {
                              ++completions[i];
                              results[i] = error;
                              if (i > 0 && ++queuedDone == 2) {
                                  // Release the held request only after the queued ones were failed.
                                  server.Stop();
                              }
                          });
    }
    client.reset();
    RunBounded(context);

    EXPECT_EQ(completions, (std::vector<int>{1, 1, 1}));
    EXPECT_EQ(results[1], std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(results[2], std::make_error_code(std::errc::operation_canceled));
    EXPECT_TRUE(results[0]);
}

TEST(HttpClientRobustness, DestroyingTheClientWithRunningRequestsCompletesEachExactlyOnce) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Hold = true;
        return action;
    });

    int completions = 0;
    for (int i = 0; i < 3; ++i) {
        AVEVA::HttpRequestOptions options;
        options.SetTimeout(std::chrono::seconds(10));
        client->SendAsync(MakeRequest(server.Url("/" + std::to_string(i))), options,
                          [&](std::error_code error, AVEVA::HttpResponse) {
                              EXPECT_TRUE(error);
                              if (++completions == 3) {
                                  server.Stop();
                              }
                          });
    }
    client.reset();
    asio::steady_timer stopper(context, std::chrono::milliseconds(100));
    stopper.async_wait([&](boost::system::error_code) { server.Stop(); });
    RunBounded(context);

    EXPECT_EQ(completions, 3);
}

TEST(HttpClientRobustness, DestroyingTheClientWhileSlotsReleaseOnAMultiThreadedContextCompletesEachOnce) {
    constexpr int Requests = 48;
    // The server runs on its own single-threaded context because ScriptedServer is not thread-safe.
    asio::io_context serverContext;
    ScriptedServer server(serverContext,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });
    auto serverGuard = asio::make_work_guard(serverContext);
    std::thread serverThread([&] { serverContext.run(); });
    for (int iteration = 0; iteration < 20; ++iteration) {
        asio::io_context context;
        AVEVA::HttpClientOptions clientOptions;
        clientOptions.SetMaxConnectionsPerHost(2);
        auto client = AVEVA::HttpClient::Create(context, clientOptions);

        // Each finished request hands its slot to the next waiter, which is what races with destruction.
        std::atomic<int> completions = 0;
        for (int i = 0; i < Requests; ++i) {
            AVEVA::HttpRequestOptions options;
            options.SetTimeout(std::chrono::seconds(10));
            client->SendAsync(MakeRequest(server.Url("/" + std::to_string(i))), options,
                              [&](std::error_code, AVEVA::HttpResponse) { ++completions; });
        }

        std::vector<std::thread> workers;
        for (int i = 0; i < 4; ++i) {
            workers.emplace_back([&] { context.run(); });
        }
        for (int spin = 0; spin < (iteration % 5) * 300; ++spin) {
            std::this_thread::yield();
        }
        client.reset();
        for (auto& worker : workers) {
            worker.join();
        }

        EXPECT_EQ(completions.load(), Requests);
    }
    asio::post(serverContext, [&] { server.Stop(); });
    serverGuard.reset();
    serverThread.join();
}

TEST(HttpClientRobustness, CancellingTheSharedDnsLeaderStillCompletesEveryJoinedRequestOnce) {
    for (int iteration = 0; iteration < 10; ++iteration) {
        asio::io_context context;
        auto client = AVEVA::HttpClient::Create(context);
        ScriptedServer server(
            context, [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });
        // The cache key includes the port, which is fresh per server, so every iteration starts a new lookup.
        const auto url = server.Url();

        asio::cancellation_signal leaderCancel;
        AVEVA::HttpRequestOptions leaderOptions;
        leaderOptions.SetCancellationSlot(leaderCancel.slot());
        int completions = 0;
        std::error_code followerResult;
        const auto finish = [&] {
            if (++completions == 3) {
                server.Stop();
            }
        };
        client->SendAsync(MakeRequest(url), leaderOptions, [&](std::error_code, AVEVA::HttpResponse) { finish(); });
        for (int follower = 0; follower < 2; ++follower) {
            client->SendAsync(MakeRequest(url), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  followerResult = error;
                                  finish();
                              });
        }
        // Queued behind the three starts, so it lands while the leader's lookup is in flight.
        asio::post(context, [&] { leaderCancel.emit(asio::cancellation_type::terminal); });
        RunBounded(context);

        EXPECT_EQ(completions, 3);
        EXPECT_NE(followerResult, std::make_error_code(std::errc::operation_canceled));
    }
}

TEST(HttpClientRobustness, MixedCaseSchemeReusesTheSameConnection) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    ScriptedServer server(
        context, [](int, const http::request<http::string_body>&) -> ServerAction { return {.Wire = OkResponse}; });
    const auto port = server.Url("/").substr(std::string("http://127.0.0.1").size());

    std::vector<std::error_code> errors;
    client->SendAsync(MakeRequest("http://127.0.0.1" + port + "a"), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          errors.push_back(error);
                          client->SendAsync(MakeRequest("HtTp://127.0.0.1" + port + "b"), AVEVA::HttpRequestOptions{},
                                            [&](std::error_code secondError, AVEVA::HttpResponse) {
                                                errors.push_back(secondError);
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_FALSE(errors[0]);
    EXPECT_FALSE(errors[1]);
    EXPECT_EQ(server.Accepted(), 1);
}

TEST(HttpClientRobustness, RouteFoldsSchemeAndDnsHostCaseButKeepsTheTarget) {
    const auto route =
        AVEVA::Private::Route::Of(MakeRequest("HTTP://Example.COM:8080/Path?Q=1"), AVEVA::HttpRequestOptions{});

    ASSERT_TRUE(route.Sendable());
    EXPECT_EQ(route.Origin(), "http://example.com:8080");
    EXPECT_EQ(route.Target, "/Path?Q=1");
}

TEST(HttpClientRobustness, RouteCanonicalizesEquivalentPortsAndIpLiteralsButKeepsHostHeaderSpelling) {
    const AVEVA::HttpRequestOptions options;
    const auto padded = AVEVA::Private::Route::Of(MakeRequest("http://example.com:080/a"), options);
    const auto plain = AVEVA::Private::Route::Of(MakeRequest("http://example.com:80/a"), options);
    const auto implicit = AVEVA::Private::Route::Of(MakeRequest("http://example.com/a"), options);
    const auto other = AVEVA::Private::Route::Of(MakeRequest("http://example.com:81/a"), options);
    ASSERT_TRUE(padded.Sendable());
    EXPECT_EQ(padded.Origin(), plain.Origin());
    EXPECT_EQ(padded.Origin(), implicit.Origin());
    EXPECT_NE(padded.Origin(), other.Origin());
    EXPECT_EQ(padded.Authority, "example.com:080");

    const auto longForm = AVEVA::Private::Route::Of(MakeRequest("http://[0:0:0:0:0:0:0:1]:0080/"), options);
    const auto shortForm = AVEVA::Private::Route::Of(MakeRequest("http://[::1]:80/"), options);
    ASSERT_TRUE(longForm.Sendable());
    EXPECT_EQ(longForm.Origin(), shortForm.Origin());
    EXPECT_EQ(longForm.Host, shortForm.Host);
    EXPECT_EQ(longForm.Authority, "[0:0:0:0:0:0:0:1]:0080");
}

TEST(HttpClientRobustness, ConnectingToAClosedPortFailsWithConnectFailed) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);

    std::error_code result;
    client->SendAsync(MakeRequest(HttpClientTests::ClosedLocalUrl()), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) { result = error; });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::ConnectFailed));
}

TEST(HttpClientRobustness, UnresolvableHostFailsWithResolveFailed) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);

    std::error_code result;
    client->SendAsync(MakeRequest("http://no-such-host.invalid/"), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) { result = error; });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::ResolveFailed));
}

TEST(HttpClientDiagnostics, DisabledPoolingDoesNotReportConnectionNotPooled) {
    asio::io_context context;
    std::vector<AVEVA::HttpDiagnostic> events;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetMaxIdleConnectionsPerHost(0);
    clientOptions.SetDiagnosticsCallback([&](const AVEVA::HttpDiagnostic& event) { events.push_back(event); });
    auto client = AVEVA::HttpClient::Create(context, std::move(clientOptions));
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) -> ServerAction { return {OkResponse}; });

    std::error_code result = AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut);
    client->SendAsync(MakeRequest(server.Url()), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse) {
                          result = error;
                          server.Stop();
                      });
    RunBounded(context);

    EXPECT_FALSE(result);
    EXPECT_TRUE(std::ranges::none_of(
        events, [](const auto& event) { return event.Kind == AVEVA::HttpDiagnosticKind::ConnectionNotPooled; }));
}

TEST(HttpClientDiagnostics, TimeoutReportsThePhaseItHappenedIn) {
    asio::io_context context;
    std::vector<AVEVA::HttpDiagnostic> events;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetDiagnosticsCallback([&](const AVEVA::HttpDiagnostic& event) { events.push_back(event); });
    auto client = AVEVA::HttpClient::Create(context, std::move(clientOptions));
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Hold = true;
        return action;
    });

    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds(200));
    std::error_code result;
    client->SendAsync(MakeRequest(server.Url()), requestOptions, [&](std::error_code error, AVEVA::HttpResponse) {
        result = error;
        server.Stop();
    });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    const auto timeouts = std::ranges::count_if(
        events, [](const auto& event) { return event.Kind == AVEVA::HttpDiagnosticKind::Timeout; });
    ASSERT_EQ(timeouts, 1);
    const auto it = std::ranges::find_if(
        events, [](const auto& event) { return event.Kind == AVEVA::HttpDiagnosticKind::Timeout; });
    EXPECT_EQ(it->Phase, AVEVA::HttpRequestPhase::Read);
    EXPECT_FALSE(it->Origin.empty());
}

TEST(HttpClientDiagnostics, ThrowingCallbackDoesNotDisturbTheRequest) {
    asio::io_context context;
    int calls = 0;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetDiagnosticsCallback([&](const AVEVA::HttpDiagnostic&) {
        ++calls;
        throw std::runtime_error("receiver failed");
    });
    auto client = AVEVA::HttpClient::Create(context, std::move(clientOptions));
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Hold = true;
        return action;
    });

    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds(200));
    std::error_code result;
    client->SendAsync(MakeRequest(server.Url()), requestOptions, [&](std::error_code error, AVEVA::HttpResponse) {
        result = error;
        server.Stop();
    });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    EXPECT_GE(calls, 1);
}

TEST(HttpClientDiagnostics, StaleReusedConnectionIsReportedAsARetry) {
    asio::io_context context;
    std::vector<AVEVA::HttpDiagnostic> events;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetDiagnosticsCallback([&](const AVEVA::HttpDiagnostic& event) { events.push_back(event); });
    auto client = AVEVA::HttpClient::Create(context, std::move(clientOptions));
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        // The first connection answers its first request, then closes silently on the second.
        if (connection == 0 && request.target() == "/second") {
            ServerAction action;
            action.Drop = true;
            return action;
        }
        return {OkResponse};
    });

    std::error_code second;
    client->SendAsync(MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                      [&](std::error_code, AVEVA::HttpResponse) {
                          client->SendAsync(MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                            [&](std::error_code error, AVEVA::HttpResponse) {
                                                second = error;
                                                server.Stop();
                                            });
                      });
    RunBounded(context);

    EXPECT_FALSE(second);
    EXPECT_TRUE(std::ranges::any_of(
        events, [](const auto& event) { return event.Kind == AVEVA::HttpDiagnosticKind::StaleConnectionRetry; }));
}
} // namespace
