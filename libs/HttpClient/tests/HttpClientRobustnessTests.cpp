#include "AVEVA/HttpClient/HttpClient.hpp"

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

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = beast::http;
    using Tcp = asio::ip::tcp;

    constexpr const char* const OkResponse = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

    struct ServerAction
    {
        std::string Wire;
        bool CloseAfterWrite = false;
        // When set, the response is withheld until the delay elapsed.
        std::chrono::milliseconds Delay{0};
        // When true, the request is read but never answered.
        bool Hold = false;
        // When true, the connection is closed right after the request was read, without any response.
        bool Drop = false;
    };

    // Accepts any number of connections and serves keep-alive requests on each; what to answer is scripted
    // by `behavior(connectionIndex, request)`.
    class ScriptedServer
    {
      public:
        using Behavior = std::function<ServerAction(int, const http::request<http::string_body>&)>;

        ScriptedServer(asio::io_context& context, Behavior behavior)
            : m_context(context), m_acceptor(context, {asio::ip::make_address("127.0.0.1"), 0}),
              m_behavior(std::move(behavior))
        {
            Accept();
        }

        std::string Url(const std::string& path = "/") const
        {
            return "http://127.0.0.1:" + std::to_string(m_acceptor.local_endpoint().port()) + path;
        }

        void Stop()
        {
            boost::system::error_code ignored;
            m_acceptor.close(ignored);
            for (const auto& connection : m_connections)
            {
                connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
                connection->Socket.close(ignored);
                connection->Timer.cancel();
            }
        }

        int Accepted() const
        {
            return m_accepted;
        }

        int Requests(http::verb method) const
        {
            int count = 0;
            for (const auto verb : m_methods)
            {
                count += verb == method ? 1 : 0;
            }
            return count;
        }

        int TotalRequests() const
        {
            return static_cast<int>(m_methods.size());
        }

      private:
        struct Connection
        {
            explicit Connection(asio::io_context& context, int connectionIndex)
                : Socket(context), Timer(context), Index(connectionIndex)
            {
            }

            Tcp::socket Socket;
            asio::steady_timer Timer;
            beast::flat_buffer Buffer;
            http::request<http::string_body> Request;
            int Index;
        };

        void Accept()
        {
            auto connection = std::make_shared<Connection>(m_context, m_accepted);
            m_acceptor.async_accept(connection->Socket,
                [this, connection](boost::system::error_code error)
            {
                if (error)
                {
                    return;
                }
                ++m_accepted;
                m_connections.push_back(connection);
                ReadNext(connection);
                Accept();
            });
        }

        void ReadNext(const std::shared_ptr<Connection>& connection)
        {
            connection->Request = {};
            http::async_read(connection->Socket,
                connection->Buffer,
                connection->Request,
                [this, connection](boost::system::error_code error, std::size_t)
            {
                if (error)
                {
                    return;
                }
                m_methods.push_back(connection->Request.method());
                auto action = std::make_shared<ServerAction>(m_behavior(connection->Index, connection->Request));
                if (action->Hold)
                {
                    return;
                }
                if (action->Drop)
                {
                    boost::system::error_code ignored;
                    connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
                    connection->Socket.close(ignored);
                    return;
                }
                connection->Timer.expires_after(action->Delay);
                connection->Timer.async_wait([this, connection, action](boost::system::error_code waitError)
                {
                    if (waitError)
                    {
                        return;
                    }
                    asio::async_write(connection->Socket,
                        asio::buffer(action->Wire),
                        [this, connection, action](boost::system::error_code writeError, std::size_t)
                    {
                        if (writeError)
                        {
                            return;
                        }
                        if (action->CloseAfterWrite)
                        {
                            boost::system::error_code ignored;
                            connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
                            connection->Socket.close(ignored);
                            return;
                        }
                        ReadNext(connection);
                    });
                });
            });
        }

        asio::io_context& m_context;
        Tcp::acceptor m_acceptor;
        Behavior m_behavior;
        int m_accepted = 0;
        std::vector<std::shared_ptr<Connection>> m_connections;
        std::vector<http::verb> m_methods;
    };

    AVEVA::HttpRequest MakeRequest(const std::string& url, AVEVA::HttpMethod method = AVEVA::HttpMethod::Get)
    {
        AVEVA::HttpRequest request;
        request.SetMethod(method);
        request.SetUrl(url);
        if (method == AVEVA::HttpMethod::Post)
        {
            request.SetBody("payload");
        }
        return request;
    }

    TEST(HttpClientRobustness, EofDelimitedResponseIsNotReturnedToThePool)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>&) -> ServerAction
        {
            if (connection == 0)
            {
                // No Content-Length and no chunking: the body ends when the server closes the connection.
                return {"HTTP/1.1 200 OK\r\n\r\nbody", true};
            }
            return {OkResponse};
        });

        std::vector<std::error_code> errors;
        std::vector<std::string> bodies;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            errors.push_back(error);
            bodies.push_back(response.GetBody());
            // A POST is never silently re-sent, so it only succeeds if it gets a fresh connection.
            client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                [&](std::error_code secondError, AVEVA::HttpResponse secondResponse)
            {
                errors.push_back(secondError);
                bodies.push_back(secondResponse.GetBody());
                server.Stop();
            });
        });
        context.run();

        ASSERT_EQ(errors.size(), 2u);
        EXPECT_FALSE(errors[0]);
        EXPECT_EQ(bodies[0], "body");
        EXPECT_FALSE(errors[1]);
        EXPECT_EQ(bodies[1], "OK");
        EXPECT_EQ(server.Accepted(), 2);
        EXPECT_EQ(server.TotalRequests(), 2);
    }

    TEST(HttpClientRobustness, ExtraBytesAfterACompleteResponseKeepTheConnectionOutOfThePool)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>&) -> ServerAction
        {
            if (connection == 0)
            {
                return {"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOKX"};
            }
            return {OkResponse};
        });

        std::vector<std::error_code> errors;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            errors.push_back(error);
            client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                [&](std::error_code secondError, AVEVA::HttpResponse secondResponse)
                {
                    errors.push_back(secondError);
                    EXPECT_EQ(secondResponse.GetStatus(), 200U);
                    server.Stop();
                });
        });
        context.run();

        ASSERT_EQ(errors.size(), 2U);
        EXPECT_FALSE(errors[0]);
        EXPECT_FALSE(errors[1]);
        EXPECT_EQ(server.Accepted(), 2);
        EXPECT_EQ(server.TotalRequests(), 2);
    }

    TEST(HttpClientRobustness, IdempotentRequestIsRetriedOnAStalePooledConnection)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>&) -> ServerAction
        {
            // The first connection is closed right after its keep-alive response, so the pooled stream is dead.
            return {OkResponse, connection == 0};
        });

        std::vector<std::error_code> errors;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            errors.push_back(error);
            // Give the server time to close its end before the pooled connection is reused.
            auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(100));
            timer->async_wait([&, timer](boost::system::error_code)
            {
                client->SendAsync(MakeRequest(server.Url("/second")),
                    [&](std::error_code secondError, AVEVA::HttpResponse secondResponse)
                {
                    errors.push_back(secondError);
                    EXPECT_EQ(secondResponse.GetStatus(), 200u);
                    server.Stop();
                });
            });
        });
        context.run();

        ASSERT_EQ(errors.size(), 2u);
        EXPECT_FALSE(errors[0]);
        EXPECT_FALSE(errors[1]);
        EXPECT_EQ(server.Accepted(), 2);
    }

    TEST(HttpClientRobustness, PostLostOnAReusedConnectionIsNotSentAgain)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>& request) -> ServerAction
        {
            ServerAction action{OkResponse};
            // The first connection receives the POST and then dies without answering it.
            action.Drop = connection == 0 && request.method() == http::verb::post;
            return action;
        });

        std::error_code postError;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code, AVEVA::HttpResponse)
        {
            client->SendAsync(MakeRequest(server.Url("/second"), AVEVA::HttpMethod::Post),
                [&](std::error_code error, AVEVA::HttpResponse)
            {
                postError = error;
                // Let a (wrong) resend arrive on a new connection before the server is stopped.
                auto settle = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(200));
                settle->async_wait([&, settle](boost::system::error_code) { server.Stop(); });
            });
        });
        context.run();

        EXPECT_TRUE(postError);
        EXPECT_EQ(server.Requests(http::verb::post), 1);
    }

    TEST(HttpClientRobustness, GetLostOnAReusedConnectionIsRetried)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>& request) -> ServerAction
        {
            ServerAction action{OkResponse};
            action.Drop = connection == 0 && request.target() == "/second";
            return action;
        });

        std::error_code secondError;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code, AVEVA::HttpResponse)
        {
            client->SendAsync(MakeRequest(server.Url("/second")),
                [&](std::error_code error, AVEVA::HttpResponse)
            {
                secondError = error;
                server.Stop();
            });
        });
        context.run();

        EXPECT_FALSE(secondError);
        EXPECT_EQ(server.Accepted(), 2);
    }

    TEST(HttpClientRobustness, PlainHttpRequestTimesOut)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int, const http::request<http::string_body>&) -> ServerAction
        {
            return {.Hold = true};
        });

        AVEVA::HttpRequestOptions options;
        options.SetTimeout(std::chrono::milliseconds(100));

        std::error_code observed;
        client->SendAsync(MakeRequest(server.Url("/timeout")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            observed = error;
            server.Stop();
        },
            options);
        context.run();

        EXPECT_EQ(observed, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
        EXPECT_EQ(server.Accepted(), 1);
    }

    TEST(HttpClientRobustness, ReusedConnectionRequestTimesOut)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int connection, const http::request<http::string_body>& request) -> ServerAction
        {
            if (connection == 0 && request.target() == "/second")
            {
                return {.Hold = true};
            }
            return {OkResponse};
        });

        AVEVA::HttpRequestOptions timeoutOptions;
        timeoutOptions.SetTimeout(std::chrono::milliseconds(100));

        std::vector<std::error_code> errors;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            errors.push_back(error);
            client->SendAsync(MakeRequest(server.Url("/second")),
                [&](std::error_code secondError, AVEVA::HttpResponse)
                {
                    errors.push_back(secondError);
                    server.Stop();
                },
                timeoutOptions);
        });
        context.run();

        ASSERT_EQ(errors.size(), 2U);
        EXPECT_FALSE(errors[0]);
        EXPECT_EQ(errors[1], AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
        EXPECT_EQ(server.Accepted(), 1);
    }

    TEST(HttpClientRobustness, MaximumRequestTimeoutDoesNotExpireImmediately)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        ScriptedServer server(context,
            [](int, const http::request<http::string_body>&) -> ServerAction
        {
            return {.Wire = OkResponse, .Delay = std::chrono::milliseconds(50)};
        });

        AVEVA::HttpRequestOptions options;
        options.SetTimeout(std::chrono::milliseconds::max());

        std::error_code observed;
        unsigned int status = 0;
        client->SendAsync(MakeRequest(server.Url("/max-timeout")),
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            observed = error;
            status = response.GetStatus();
            server.Stop();
        },
            options);
        context.run();

        EXPECT_FALSE(observed);
        EXPECT_EQ(status, 200U);
    }

    TEST(HttpClientRobustness, IdleTimeoutExpiryClosesThePooledConnection)
    {
        asio::io_context context;
        AVEVA::HttpClientOptions options;
        options.SetIdleConnectionTimeout(std::chrono::seconds(1));
        auto client = AVEVA::IHttpClient::Create(context, options);
        ScriptedServer server(context,
            [](int, const http::request<http::string_body>&) -> ServerAction
        {
            return {OkResponse};
        });

        std::vector<std::error_code> errors;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            errors.push_back(error);
            auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(1200));
            timer->async_wait([&, timer](boost::system::error_code waitError)
            {
                ASSERT_FALSE(waitError);
                client->SendAsync(MakeRequest(server.Url("/second")),
                    [&](std::error_code secondError, AVEVA::HttpResponse)
                    {
                        errors.push_back(secondError);
                        server.Stop();
                    });
            });
        });
        context.run();

        ASSERT_EQ(errors.size(), 2U);
        EXPECT_FALSE(errors[0]);
        EXPECT_FALSE(errors[1]);
        EXPECT_EQ(server.Accepted(), 2);
    }

    TEST(HttpClientRobustness, MaximumIdleTimeoutStillAllowsPooling)
    {
        asio::io_context context;
        AVEVA::HttpClientOptions options;
        options.SetIdleConnectionTimeout(std::chrono::seconds::max());
        auto client = AVEVA::IHttpClient::Create(context, options);
        ScriptedServer server(context,
            [](int, const http::request<http::string_body>&) -> ServerAction
        {
            return {OkResponse};
        });

        std::vector<std::error_code> errors;
        client->SendAsync(MakeRequest(server.Url("/first")),
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            errors.push_back(error);
            client->SendAsync(MakeRequest(server.Url("/second")),
                [&](std::error_code secondError, AVEVA::HttpResponse)
                {
                    errors.push_back(secondError);
                    server.Stop();
                });
        });
        context.run();

        ASSERT_EQ(errors.size(), 2U);
        EXPECT_FALSE(errors[0]);
        EXPECT_FALSE(errors[1]);
        EXPECT_EQ(server.Accepted(), 1);
    }

    TEST(HttpClientRobustness, CompletionRunsWhenTheHandlerExecutorHasNoOtherWork)
    {
        asio::io_context clientContext;
        asio::io_context handlerContext;
        auto client = AVEVA::IHttpClient::Create(clientContext);
        ScriptedServer server(clientContext,
            [](int, const http::request<http::string_body>&) -> ServerAction
        {
            ServerAction action{OkResponse};
            action.Delay = std::chrono::milliseconds(150);
            return action;
        });

        std::atomic_bool completed = false;
        std::error_code result;
        client->SendAsync(MakeRequest(server.Url()),
            AVEVA::HttpRequestOptions{},
            asio::bind_executor(handlerContext.get_executor(),
                [&](std::error_code error, AVEVA::HttpResponse)
                {
                    result = error;
                    completed = true;
                    server.Stop();
                }));

        std::thread clientThread([&] { clientContext.run(); });
        // Without outstanding work on the handler's executor this would return immediately.
        handlerContext.run();
        clientThread.join();

        EXPECT_TRUE(completed);
        EXPECT_FALSE(result);
    }

    TEST(HttpClientRobustness, CancellationFromAnotherThreadCompletesExactlyOnce)
    {
        for (int iteration = 0; iteration < 25; ++iteration)
        {
            asio::io_context context;
            auto client = AVEVA::IHttpClient::Create(context);
            ScriptedServer server(context,
                [](int, const http::request<http::string_body>&) -> ServerAction
            {
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
            client->SendAsync(MakeRequest(server.Url()),
                [&](std::error_code error, AVEVA::HttpResponse)
            {
                ++completions;
                result = error;
                server.Stop();
            },
                options);

            std::thread canceller(
                [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(iteration % 5));
                cancellation.emit(asio::cancellation_type::terminal);
            });
            context.run();
            canceller.join();

            EXPECT_EQ(completions, 1);
            EXPECT_EQ(result, std::make_error_code(std::errc::operation_canceled));
        }
    }

    TEST(HttpClientRobustness, CancellationDuringStaleConnectionRetryCompletesExactlyOnce)
    {
        for (int iteration = 0; iteration < 25; ++iteration)
        {
            asio::io_context context;
            auto client = AVEVA::IHttpClient::Create(context);
            ScriptedServer server(context,
                [](int connection, const http::request<http::string_body>&) -> ServerAction
            {
                return {OkResponse, connection == 0};
            });

            asio::cancellation_signal cancellation;
            int completions = 0;
            std::error_code secondResult;
            client->SendAsync(MakeRequest(server.Url("/first")),
                [&](std::error_code, AVEVA::HttpResponse)
            {
                auto timer = std::make_shared<asio::steady_timer>(context, std::chrono::milliseconds(50));
                timer->async_wait([&, timer](boost::system::error_code)
                {
                    AVEVA::HttpRequestOptions options;
                    options.SetCancellationSlot(cancellation.slot());
                    client->SendAsync(MakeRequest(server.Url("/second")),
                        [&](std::error_code error, AVEVA::HttpResponse)
                    {
                        ++completions;
                        secondResult = error;
                        server.Stop();
                    },
                        options);
                    // Lands while the reuse failure and the replacement connection are being processed.
                    auto cancelTimer = std::make_shared<asio::steady_timer>(
                        context, std::chrono::microseconds(iteration * 200));
                    cancelTimer->async_wait([&, cancelTimer](boost::system::error_code)
                    {
                        cancellation.emit(asio::cancellation_type::terminal);
                    });
                });
            });
            context.run();

            EXPECT_EQ(completions, 1);
            if (secondResult)
            {
                EXPECT_EQ(secondResult, std::make_error_code(std::errc::operation_canceled));
            }
        }
    }
} // namespace
