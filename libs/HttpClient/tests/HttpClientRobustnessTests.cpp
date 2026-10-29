// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/DnsCache.hpp"
#include "private/Exchange.hpp"

#include <gtest/gtest.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse response) {
                                  errors.push_back(error);
                                  bodies.push_back(response.GetBody());
                                  // A POST is never silently re-sent, so it only succeeds if it gets a fresh
                                  // connection.
                                  HttpClientTests::SendWith(
                                      *client, MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  errors.push_back(error);
                                  HttpClientTests::SendWith(
                                      *client, MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
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
    HttpClientTests::SendWith(
        *client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
        [&](std::error_code error, AVEVA::HttpResponse) {
            errors.push_back(error);
            // Give the server time to close its end before the pooled connection is reused.
            auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(100));
            timer->async_wait([&, timer](boost::system::error_code) {
                HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  HttpClientTests::SendWith(
                                      *client, MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                                      AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
                                          postError = error;
                                          // Let a (wrong) resend arrive on a new connection before the server is
                                          // stopped.
                                          auto settle = std::make_shared<asio::steady_timer>(
                                              context, std::chrono::milliseconds(200));
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  HttpClientTests::SendWith(
                                      *client, MakeRequest(server.Url("/second"), method), AVEVA::HttpRequestOptions{},
                                      [&](std::error_code error, AVEVA::HttpResponse) {
                                          mutationError = error;
                                          auto settle = std::make_shared<asio::steady_timer>(
                                              context, std::chrono::milliseconds(200));
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")),
                                                            AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  HttpClientTests::SendWith(
                                      *client, MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
                                      [&](std::error_code error, AVEVA::HttpResponse) {
                                          secondError = error;
                                          auto settle = std::make_shared<asio::steady_timer>(
                                              context, std::chrono::milliseconds(200));
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/timeout")), options,
                              [&](std::error_code error, AVEVA::HttpResponse) {
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
    HttpClientTests::SendWith(*client, std::move(request), options, [&](std::error_code error, AVEVA::HttpResponse) {
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

    int completions = 0;
    std::error_code observed;
    HttpClientTests::SendWith(
        *client, std::move(request), options,
        [&](std::error_code error, AVEVA::HttpResponse) {
            ++completions;
            observed = error;
            body.reset();
            acceptor.close();
            accepted.close();
        },
        cancellation.slot());
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  errors.push_back(error);
                                  HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")), timeoutOptions,
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/max-timeout")), options,
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
    HttpClientTests::SendWith(
        *client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
        [&](std::error_code error, AVEVA::HttpResponse) {
            errors.push_back(error);
            auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(1200));
            timer->async_wait([&, timer](boost::system::error_code waitError) {
                ASSERT_FALSE(waitError);
                HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")), AVEVA::HttpRequestOptions{},
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
        HttpClientTests::SendWith(*client, MakeRequest(server.Url("/" + std::to_string(i))),
                                  AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
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

static void RunQueuedRequestWhileHandlerBlocks(bool pooled) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetMaxConnectionsPerHost(1);
    if (!pooled) {
        options.SetMaxIdleConnectionsPerHost(0);
    }
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
    HttpClientTests::SendWith(
        *client, MakeRequest(server.Url("/a")), AVEVA::HttpRequestOptions{}, [&](std::error_code, AVEVA::HttpResponse) {
            // The slot must already be free here, so the queued request can finish on another thread
            // while this handler is still blocked.
            startedWhileBlocked = secondFuture.wait_for(std::chrono::seconds(4)) == std::future_status::ready;
            ++completed;
            guard.reset();
        });
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/b")), AVEVA::HttpRequestOptions{},
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

TEST(HttpClientRobustness, QueuedRequestStartsWhileThePreviousHandlerIsStillRunning) {
    RunQueuedRequestWhileHandlerBlocks(false);
}

// The pooled connection's strand must not be held by the blocked handler of the request that released it.
TEST(HttpClientRobustness, QueuedRequestOnAPooledConnectionStartsWhileThePreviousHandlerIsStillRunning) {
    RunQueuedRequestWhileHandlerBlocks(true);
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/held")), holdingOptions,
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  firstResult = error;
                                  finishWhenBothDone();
                              });
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/queued")), queuedOptions,
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
    asio::cancellation_signal queuedCancel;

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
    HttpClientTests::SendWith(
        *client, MakeRequest(server.Url("/held")), AVEVA::HttpRequestOptions{},
        [&](std::error_code error, AVEVA::HttpResponse) {
            firstResult = error;
            finishWhenBothDone();
        },
        firstCancel.slot());
    HttpClientTests::SendWith(
        *client, MakeRequest(server.Url("/queued")), AVEVA::HttpRequestOptions{},
        [&](std::error_code error, AVEVA::HttpResponse) {
            queuedResult = error;
            // The queued request is cancelled first; the held one is released only after that
            // completion.
            firstCancel.emit(asio::cancellation_type::terminal);
            finishWhenBothDone();
        },
        queuedCancel.slot());
    // Let both requests reach their first suspension so the queued one is parked in the gate.
    context.poll();
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  errors.push_back(error);
                                  HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")),
                                                            AVEVA::HttpRequestOptions{},
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
        options.SetTimeout(std::chrono::seconds(10));

        int completions = 0;
        std::error_code result;
        HttpClientTests::SendWith(
            *client, MakeRequest(server.Url()), options,
            [&](std::error_code error, AVEVA::HttpResponse) {
                ++completions;
                result = error;
                server.Stop();
            },
            cancellation.slot());

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
        HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                                  [&](std::error_code, AVEVA::HttpResponse) {
                                      auto timer =
                                          std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(50));
                                      timer->async_wait([&, timer](boost::system::error_code) {
                                          AVEVA::HttpRequestOptions options;
                                          HttpClientTests::SendWith(
                                              *client, MakeRequest(server.Url("/second")), options,
                                              [&](std::error_code error, AVEVA::HttpResponse) {
                                                  ++completions;
                                                  secondResult = error;
                                                  server.Stop();
                                              },
                                              cancellation.slot());
                                          // Lands while the reuse failure and the replacement connection are being
                                          // processed.
                                          auto cancelTimer = std::make_shared<asio::steady_timer>(
                                              context, std::chrono::microseconds(iteration * 200));
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url()), options,
                              [&](std::error_code error, AVEVA::HttpResponse) {
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  result = error;
                                  // The connection is now idle in the pool; tear the client down with it inside.
                                  client.reset();
                                  auto settle =
                                      std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(100));
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
        HttpClientTests::SendWith(*client, MakeRequest("http://shared-lookup.invalid/" + std::to_string(i)),
                                  AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
                                      EXPECT_TRUE(error);
                                      ++completions;
                                  });
    }
    // Let both coroutines start (and reach the lookup) before the client goes away; a not-yet-started coroutine
    // would otherwise touch a destroyed client.
    context.run_one();
    context.run_one();
    client.reset();
    RunBounded(context);

    EXPECT_EQ(completions, 2);
}

TEST(HttpClientRobustness, StartedDnsLeaderKeepsItsPendingEntryWhileTheLookupIsInFlight) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    HttpClientTests::SendWith(*client, MakeRequest("http://leader-guard.invalid/"), AVEVA::HttpRequestOptions{},
                              [](std::error_code, AVEVA::HttpResponse) {});
    // Run the coroutine's start so the request has been submitted; the probe below is then queued behind the
    // request's own start.
    context.run_one();

    bool probedAsLeader = true;
    // Queued behind the request's start, before the resolver thread can post its completion.
    asio::post(context, [&] {
        probedAsLeader = AVEVA::Private::DnsCache::Shared().JoinOrLead(
            "leader-guard.invalid", "80",
            [&context] {
                return AVEVA::Private::ResolveWaiter(
                    std::make_shared<AVEVA::Private::ResolveChannel>(context.get_executor(), 1));
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
        HttpClientTests::SendWith(*client, MakeRequest("http://127.0.0.1:" + port + paths[next++]),
                                  AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
                                      EXPECT_FALSE(error);
                                      sendNext();
                                  });
    };
    sendNext();
    RunBounded(context);

    EXPECT_EQ(targets, (std::vector<std::string>{"/", "/?a=1", "/p/q%20r?x=%2F&y=", "/p?"}));
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
        HttpClientTests::SendWith(*client, MakeRequest(server.Url("/" + std::to_string(i))), options,
                                  [&, i](std::error_code error, AVEVA::HttpResponse) {
                                      ++completions[i];
                                      results[i] = error;
                                      if (i > 0 && ++queuedDone == 2) {
                                          // Release the held request only after the queued ones were failed.
                                          server.Stop();
                                      }
                                  });
    }
    // The spawned coroutines must have started before the client is destroyed, or they would use a dead client.
    for (int i = 0; i < 3; ++i) {
        context.run_one();
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
        HttpClientTests::SendWith(*client, MakeRequest(server.Url("/" + std::to_string(i))), options,
                                  [&](std::error_code error, AVEVA::HttpResponse) {
                                      EXPECT_TRUE(error);
                                      if (++completions == 3) {
                                          server.Stop();
                                      }
                                  });
    }
    for (int i = 0; i < 3; ++i) {
        context.run_one();
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
            HttpClientTests::SendWith(*client, MakeRequest(server.Url("/" + std::to_string(i))), options,
                                      [&](std::error_code, AVEVA::HttpResponse) { ++completions; });
        }
        // Start every spawned coroutine before the workers run, so destruction cannot overtake a pending start.
        for (int i = 0; i < Requests; ++i) {
            context.run_one();
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
        int completions = 0;
        std::error_code followerResult;
        const auto finish = [&] {
            if (++completions == 3) {
                server.Stop();
            }
        };
        HttpClientTests::SendWith(
            *client, MakeRequest(url), AVEVA::HttpRequestOptions{},
            [&](std::error_code, AVEVA::HttpResponse) { finish(); }, leaderCancel.slot());
        for (int follower = 0; follower < 2; ++follower) {
            HttpClientTests::SendWith(*client, MakeRequest(url), AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest("http://127.0.0.1" + port + "a"), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  errors.push_back(error);
                                  HttpClientTests::SendWith(*client, MakeRequest("HtTp://127.0.0.1" + port + "b"),
                                                            AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest(HttpClientTests::ClosedLocalUrl()), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) { result = error; });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::ConnectFailed));
}

TEST(HttpClientRobustness, UnresolvableHostFailsWithResolveFailed) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);

    std::error_code result;
    HttpClientTests::SendWith(*client, MakeRequest("http://no-such-host.invalid/"), AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url()), AVEVA::HttpRequestOptions{},
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url()), requestOptions,
                              [&](std::error_code error, AVEVA::HttpResponse) {
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url()), requestOptions,
                              [&](std::error_code error, AVEVA::HttpResponse) {
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
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/first")), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  HttpClientTests::SendWith(*client, MakeRequest(server.Url("/second")),
                                                            AVEVA::HttpRequestOptions{},
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
