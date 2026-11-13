// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/Exchange.hpp"

#include <gtest/gtest.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <variant>
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
using HttpClientTests::ServerReply;

AVEVA::HttpRequest MakeRequest(const std::string& url, AVEVA::HttpMethod method = AVEVA::HttpMethod::Get) {
    AVEVA::HttpRequest request;
    request.SetMethod(method);
    request.SetUrl(AVEVA::HttpUrl{url});
    if (method == AVEVA::HttpMethod::Post) {
        request.SetBody(AVEVA::HttpBody{"payload"});
    }
    return request;
}

TEST(HttpClientRobustness, EofDelimitedResponseIsNotReturnedToThePool) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
        if (connection == 0) {
            // No Content-Length and no chunking: the body ends when the server closes the connection.
            return {"HTTP/1.1 200 OK\r\n\r\nbody", ServerReply::RespondAndClose};
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
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
                                          EXPECT_EQ(secondResponse.GetStatus(), AVEVA::HttpStatusCode{200});
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
        // The first connection is closed right after its keep-alive response, so the pooled stream is dead.
        return {OkResponse, connection == 0 ? ServerReply::RespondAndClose : ServerReply::Respond};
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
                                              EXPECT_EQ(secondResponse.GetStatus(), AVEVA::HttpStatusCode{200});
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        ServerAction action{OkResponse};
        // The first connection receives the POST and then dies without answering it.
        if (connection == 0 && request.method() == http::verb::post) {
            action.Reply = ServerReply::Drop;
        }
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context,
                          [verb](int connection, const http::request<http::string_body>& request) -> ServerAction {
                              ServerAction action{OkResponse};
                              if (connection == 0 && request.method() == verb) {
                                  action.Reply = ServerReply::Drop;
                              }
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        ServerAction action{OkResponse};
        if (connection == 0 && request.target() == "/second") {
            action.Reply = ServerReply::Drop;
        }
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        if (connection == 0 && request.target() == "/second") {
            // The server accepted the request and started answering before dying.
            return {"HTTP/1.1 200 OK\r\nContent-Le", ServerReply::RespondAndClose};
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
    requestOptions.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds(100)});
    auto result = HttpClientTests::Exchange({}, {}, requestOptions, ServerReply::Hold);

    EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
}

TEST(HttpClientRobustness, PlainHttpRequestTimesOut) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Reply = ServerReply::Hold};
    });

    AVEVA::HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds(100)});

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

TEST(HttpClientRobustness, TimeoutOfAStalledBorrowedUploadCompletesOnceAndLetsTheCallerFreeTheBody) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    // Accepts the connection but never reads, so the client's upload stalls once the socket buffers fill.
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    Tcp::socket accepted(context);
    acceptor.async_accept(accepted, [](boost::system::error_code) {});

    auto body = std::make_shared<std::vector<std::byte>>(64U * 1024U * 1024U, std::byte{0x41});
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Put);
    request.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/"});
    request.SetBody(AVEVA::HttpBody(*body, body));
    AVEVA::HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds(300)});

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

TEST(HttpClientRobustness, CancellingAStalledBorrowedUploadCompletesOnceAndLetsTheCallerFreeTheBody) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    Tcp::socket accepted(context);
    acceptor.async_accept(accepted, [](boost::system::error_code) {});

    auto body = std::make_shared<std::vector<std::byte>>(64U * 1024U * 1024U, std::byte{0x41});
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Put);
    request.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/"});
    request.SetBody(AVEVA::HttpBody(*body, body));
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        if (connection == 0 && request.target() == "/second") {
            return {.Reply = ServerReply::Hold};
        }
        return {OkResponse};
    });

    AVEVA::HttpRequestOptions timeoutOptions;
    timeoutOptions.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds(100)});

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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(50)};
    });

    AVEVA::HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds::max()});

    std::error_code observed;
    AVEVA::HttpStatusCode status;
    HttpClientTests::SendWith(*client, MakeRequest(server.Url("/max-timeout")), options,
                              [&](std::error_code error, AVEVA::HttpResponse response) {
                                  observed = error;
                                  status = response.GetStatus();
                                  server.Stop();
                              });
    RunBounded(context);

    EXPECT_FALSE(observed);
    EXPECT_EQ(status, AVEVA::HttpStatusCode{200});
}

TEST(HttpClientRobustness, IdleTimeoutExpiryClosesThePooledConnection) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetIdleConnectionTimeout(AVEVA::Timeout{std::chrono::seconds(1)});
    auto client = std::make_unique<AVEVA::HttpClient>(context, options);
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

TEST(HttpClientRobustness, MaximumIdleTimeoutStillAllowsPooling) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetIdleConnectionTimeout(AVEVA::Timeout{std::chrono::seconds::max()});
    auto client = std::make_unique<AVEVA::HttpClient>(context, options);
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
        auto client = std::make_unique<AVEVA::HttpClient>(context);
        ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
            ServerAction action;
            action.Reply = ServerReply::Hold;
            return action;
        });

        asio::cancellation_signal cancellation;
        AVEVA::HttpRequestOptions options;
        options.SetTimeout(AVEVA::Timeout{std::chrono::seconds(10)});

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
        auto client = std::make_unique<AVEVA::HttpClient>(context);
        ScriptedServer server(context, [](int connection, const http::request<http::string_body>&) -> ServerAction {
            return {OkResponse, connection == 0 ? ServerReply::RespondAndClose : ServerReply::Respond};
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) -> ServerAction {
        return {wire, ServerReply::RespondAndClose};
    });
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
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
    auto client = std::make_unique<AVEVA::HttpClient>(context);
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

TEST(HttpClientRobustness, DestroyingTheClientBeforeTheIoContextRunsDoesNotTouchAFreedClient) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);

    // SendAsync must copy what it needs from the client eagerly, synchronously, when called here: co_spawn's
    // awaitable starts suspended and does not run any of its body until the context below processes it, by which
    // time the client is already destroyed.
    auto sendAwaitable = client->SendAsync(MakeRequest("http://no-such-host.invalid/"), AVEVA::HttpRequestOptions{});

    bool completed = false;
    asio::co_spawn(context, std::move(sendAwaitable), [&](std::exception_ptr error, AVEVA::HttpResult) {
        EXPECT_FALSE(error);
        completed = true;
    });
    client.reset();

    RunBounded(context);

    EXPECT_TRUE(completed);
}

TEST(HttpClientRobustness, RefusedConnectionCarriesTheTransportCause) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    // Bind a port and close it again so nothing listens there.
    std::uint16_t port = 0;
    {
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        port = acceptor.local_endpoint().port();
    }

    auto future =
        asio::co_spawn(context, client->SendAsync(MakeRequest("http://127.0.0.1:" + std::to_string(port) + "/"), {}),
                       asio::use_future);
    RunBounded(context);

    const auto result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Error(), AVEVA::make_error_code(AVEVA::HttpClientError::ConnectFailed));
    EXPECT_EQ(result.error().Cause().Error(),
              std::error_code(boost::system::error_code(asio::error::connection_refused)));
}

TEST(HttpClientRobustness, OversizedBodyIsReportedAsResponseTooLarge) {
    AVEVA::HttpRequestOptions options;
    options.SetResponseBodyLimit(AVEVA::ByteCount{4});
    EXPECT_EQ(SendOnce("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789", options),
              AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
}

TEST(HttpClientRobustness, TruncatedMessageIsAReadFailureNotAProtocolError) {
    EXPECT_EQ(SendOnce("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nOK"),
              AVEVA::make_error_code(AVEVA::HttpClientError::ReadFailed));
}
TEST(HttpClientRobustness, RequestTargetIsBuiltFromPathAndQuery) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
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

TEST(HttpClientRobustness, DestroyingTheClientWithRunningRequestsCompletesEachExactlyOnce) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Reply = ServerReply::Hold;
        return action;
    });

    int completions = 0;
    for (int i = 0; i < 3; ++i) {
        AVEVA::HttpRequestOptions options;
        options.SetTimeout(AVEVA::Timeout{std::chrono::seconds(10)});
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

TEST(HttpClientRobustness, MixedCaseSchemeReusesTheSameConnection) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
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
    const auto route = AVEVA::Private::Route::Of(MakeRequest("HTTP://Example.COM:8080/Path?Q=1"));

    ASSERT_EQ(route.Sendable(), AVEVA::Private::Sendability::Sendable);
    EXPECT_EQ(route.Origin(), "http://example.com:8080");
    EXPECT_EQ(route.Target, "/Path?Q=1");
}

TEST(HttpClientRobustness, RouteCanonicalizesEquivalentPortsAndIpLiteralsButKeepsHostHeaderSpelling) {
    const AVEVA::HttpRequestOptions options;
    const auto padded = AVEVA::Private::Route::Of(MakeRequest("http://example.com:080/a"));
    const auto plain = AVEVA::Private::Route::Of(MakeRequest("http://example.com:80/a"));
    const auto implicit = AVEVA::Private::Route::Of(MakeRequest("http://example.com/a"));
    const auto other = AVEVA::Private::Route::Of(MakeRequest("http://example.com:81/a"));
    ASSERT_EQ(padded.Sendable(), AVEVA::Private::Sendability::Sendable);
    EXPECT_EQ(padded.Origin(), plain.Origin());
    EXPECT_EQ(padded.Origin(), implicit.Origin());
    EXPECT_NE(padded.Origin(), other.Origin());
    EXPECT_EQ(padded.Authority, "example.com:080");

    const auto longForm = AVEVA::Private::Route::Of(MakeRequest("http://[0:0:0:0:0:0:0:1]:0080/"));
    const auto shortForm = AVEVA::Private::Route::Of(MakeRequest("http://[::1]:80/"));
    ASSERT_EQ(longForm.Sendable(), AVEVA::Private::Sendability::Sendable);
    EXPECT_EQ(longForm.Origin(), shortForm.Origin());
    EXPECT_EQ(longForm.Host, shortForm.Host);
    EXPECT_EQ(longForm.Authority, "[0:0:0:0:0:0:0:1]:0080");
}

TEST(HttpClientRobustness, ConnectingToAClosedPortFailsWithConnectFailed) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);

    std::error_code result;
    HttpClientTests::SendWith(*client, MakeRequest(HttpClientTests::ClosedLocalUrl()), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) { result = error; });
    RunBounded(context);

    EXPECT_EQ(result, AVEVA::make_error_code(AVEVA::HttpClientError::ConnectFailed));
}

TEST(HttpClientRobustness, UnresolvableHostFailsWithResolveFailed) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);

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
    clientOptions.SetMaxIdleConnectionsPerHost(AVEVA::ConnectionCount{0});
    clientOptions.SetDiagnosticsCallback(
        AVEVA::HttpDiagnosticsCallback{[&](const AVEVA::HttpDiagnostic& event) noexcept { events.push_back(event); }});
    auto client = std::make_unique<AVEVA::HttpClient>(context, std::move(clientOptions));
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
    clientOptions.SetDiagnosticsCallback(
        AVEVA::HttpDiagnosticsCallback{[&](const AVEVA::HttpDiagnostic& event) noexcept { events.push_back(event); }});
    auto client = std::make_unique<AVEVA::HttpClient>(context, std::move(clientOptions));
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) -> ServerAction {
        ServerAction action;
        action.Reply = ServerReply::Hold;
        return action;
    });

    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds(200)});
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
    EXPECT_FALSE(it->Origin.Value().empty());
}

TEST(HttpClientDiagnostics, StaleReusedConnectionIsReportedAsARetry) {
    asio::io_context context;
    std::vector<AVEVA::HttpDiagnostic> events;
    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetDiagnosticsCallback(
        AVEVA::HttpDiagnosticsCallback{[&](const AVEVA::HttpDiagnostic& event) noexcept { events.push_back(event); }});
    auto client = std::make_unique<AVEVA::HttpClient>(context, std::move(clientOptions));
    ScriptedServer server(context, [](int connection, const http::request<http::string_body>& request) -> ServerAction {
        // The first connection answers its first request, then closes silently on the second.
        if (connection == 0 && request.target() == "/second") {
            ServerAction action;
            action.Reply = ServerReply::Drop;
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
