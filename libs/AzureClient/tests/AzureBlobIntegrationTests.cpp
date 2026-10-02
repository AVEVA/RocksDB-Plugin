// Live integration tests that exercise BlobContainerClient, BlockBlobClient, and
// PageBlobClient against a real Azure Blob Storage account. These tests are skipped
// automatically unless AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID,
// AZURE_SERVICE_PRINCIPAL_ID, and AZURE_SERVICE_PRINCIPAL_SECRET are set in the environment.
// Authentication is done via Microsoft Entra ID (Azure AD) using the service principal's
// client-credentials OAuth2 flow; the resulting bearer token is used to create/delete a
// uniquely named container for the duration of the run.
//
// Run just these tests with: aveva-azure-client-integration-tests --gtest_filter=AzureBlobIntegrationTest.*

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "AzureIntegrationTestHelpers.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/system/error_code.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <exception>
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace
{
    using AVEVA::HttpClientOptions;
    using AVEVA::HttpResponse;
    using AVEVA::IHttpClient;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::BlobProperties;
    using AVEVA::AzureClient::Models::BlobType;
    using AVEVA::AzureClient::Models::BreakBlobLeaseResult;
    using AVEVA::AzureClient::Models::CommitBlockListResult;
    using AVEVA::AzureClient::Models::CreateBlobContainerResult;
    using AVEVA::AzureClient::Models::CreatePageBlobResult;
    using AVEVA::AzureClient::Models::DeleteBlobContainerResult;
    using AVEVA::AzureClient::Models::DeleteBlobResult;
    using AVEVA::AzureClient::Models::ListBlobsResult;
    using AVEVA::AzureClient::Models::ResizePageBlobResult;
    using AVEVA::AzureClient::Models::StageBlockResult;
    using AVEVA::AzureClient::Models::UploadBlockBlobResult;
    using AVEVA::AzureClient::Models::UploadPagesResult;

    constexpr std::string_view ApiVersion = "2023-11-03";

    // Drives a single async client operation to completion on `context` with a watchdog timer,
    // so a hung network call fails with a useful operation-specific error instead of waiting for
    // the outer CTest timeout. All client classes now complete with a single
    // std::expected<Response<T>, BlobStorageError> instead of a (std::error_code, Response<T>)
    // pair; RunSync bridges that back into the legacy pair shape so its many call sites below
    // (which destructure into `[error, response]` and check `error`/`error.message()`) are
    // unaffected. On failure, the original raw HttpResponse isn't retained by std::expected's
    // error path, so an empty HttpResponse is used instead (matching the client classes' own
    // internal LegacyAdapter behavior).
    template <class T, class Op>
    std::pair<std::error_code, Response<T>> RunSync(boost::asio::io_context& context,
        std::string_view operationName,
        Op&& op,
        std::chrono::seconds timeout = std::chrono::seconds{30})
    {
        std::optional<std::expected<Response<T>, BlobStorageError>> result;
        bool timedOut = false;
        boost::asio::steady_timer watchdog(context);
        watchdog.expires_after(timeout);
        watchdog.async_wait([&](const boost::system::error_code& timerError)
        {
            if (!timerError && !result.has_value())
            {
                timedOut = true;
                context.stop();
            }
        });

        std::forward<Op>(op)([&](std::expected<Response<T>, BlobStorageError> opResult)
        {
            result = std::move(opResult);
            watchdog.cancel();
        });

        context.run();
        context.restart();

        if (timedOut)
        {
            throw std::runtime_error(
                std::string(operationName) + " timed out after " + std::to_string(timeout.count()) + " seconds.");
        }
        if (!result.has_value())
        {
            throw std::runtime_error(std::string(operationName) + " never completed.");
        }

        if (result->has_value())
        {
            return {std::error_code{}, std::move(**result)};
        }

        BlobStorageError error = std::move(result->error());
        const std::error_code errorCode = error.Code ? error.Code : std::make_error_code(std::errc::io_error);
        return {errorCode, Response<T>{{}, HttpResponse{}, std::move(error)}};
    }

    [[nodiscard]] std::pair<std::error_code, Response<StageBlockResult>> RunStageBlock(boost::asio::io_context& context,
        BlockBlobClient& client,
        std::string_view operationName,
        const std::string& blockId,
        const std::string& content)
    {
        return RunSync<StageBlockResult>(context,
            operationName,
            [&](auto completion)
        {
            client.StageBlockAsync(blockId, content, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<CommitBlockListResult>> RunCommitBlockList(
        boost::asio::io_context& context,
        BlockBlobClient& client,
        std::string_view operationName,
        std::initializer_list<std::string> blockIds)
    {
        return RunSync<CommitBlockListResult>(context,
            operationName,
            [&](auto completion)
        {
            client.CommitBlockListAsync(blockIds, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<BlobProperties>> RunGetBlobProperties(
        boost::asio::io_context& context,
        BlockBlobClient& client,
        std::string_view operationName)
    {
        return RunSync<BlobProperties>(context,
            operationName,
            [&](auto completion)
        {
            client.GetPropertiesAsync(std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<DeleteBlobResult>> RunDeleteBlockBlob(
        boost::asio::io_context& context,
        BlockBlobClient& client,
        std::string_view operationName)
    {
        return RunSync<DeleteBlobResult>(context,
            operationName,
            [&](auto completion)
        {
            client.DeleteAsync(std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<CreatePageBlobResult>> RunCreatePageBlob(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName,
        std::uint64_t size)
    {
        return RunSync<CreatePageBlobResult>(context,
            operationName,
            [&](auto completion)
        {
            client.CreateAsync(size, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<UploadPagesResult>> RunUploadPages(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName,
        std::uint64_t offset,
        const std::string& content)
    {
        return RunSync<UploadPagesResult>(context,
            operationName,
            [&](auto completion)
        {
            client.UploadPagesAsync(offset, content, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<AVEVA::AzureClient::Models::ClearPagesResult>> RunClearPages(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName,
        std::uint64_t offset,
        std::uint64_t length)
    {
        return RunSync<AVEVA::AzureClient::Models::ClearPagesResult>(context,
            operationName,
            [&](auto completion)
        {
            client.ClearPagesAsync(offset, length, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<ResizePageBlobResult>> RunResizePageBlob(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName,
        std::uint64_t size)
    {
        return RunSync<ResizePageBlobResult>(context,
            operationName,
            [&](auto completion)
        {
            client.ResizeAsync(size, std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<BlobProperties>> RunGetPageBlobProperties(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName)
    {
        return RunSync<BlobProperties>(context,
            operationName,
            [&](auto completion)
        {
            client.GetPropertiesAsync(std::move(completion));
        });
    }

    [[nodiscard]] std::pair<std::error_code, Response<DeleteBlobResult>> RunDeletePageBlob(
        boost::asio::io_context& context,
        PageBlobClient& client,
        std::string_view operationName)
    {
        return RunSync<DeleteBlobResult>(context,
            operationName,
            [&](auto completion)
        {
            client.DeleteAsync(std::move(completion));
        });
    }

    class AzureBlobIntegrationTest : public ::testing::Test
    {
      protected:
        void SetUp() override
        {
            auto config = AVEVA::AzureClient::IntegrationTests::LoadAzureTestConfig();
            if (!config.has_value())
            {
                GTEST_SKIP() << "Set AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID, and "
                                "AZURE_SERVICE_PRINCIPAL_SECRET to run live Azure Blob Storage integration tests.";
            }

            m_caBundle = AVEVA::AzureClient::IntegrationTests::ExportSystemCaBundle();
            HttpClientOptions httpClientOptions;
            if (m_caBundle.has_value())
            {
                httpClientOptions.SetCaFile(m_caBundle->Path);
            }

            const std::string caFile = m_caBundle.has_value() ? m_caBundle->Path : std::string{};
            auto bearerToken = AVEVA::AzureClient::IntegrationTests::AcquireAadAccessToken(*config, caFile);
            if (!bearerToken.has_value())
            {
                GTEST_SKIP() << "Failed to acquire a Microsoft Entra ID access token for the configured service "
                                "principal; check AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID, and "
                                "AZURE_SERVICE_PRINCIPAL_SECRET.";
            }

            m_httpClient = IHttpClient::Create(m_context, httpClientOptions);

            BlobContainerClientOptions options{
                .ServiceEndpoint = config->ServiceEndpoint,
                .ContainerName = AVEVA::AzureClient::IntegrationTests::GenerateUniqueContainerName("aveva-it-"),
                .BearerToken = *bearerToken,
                .ApiVersion = std::string(ApiVersion),
            };

            m_containerClient = std::make_unique<BlobContainerClient>(*m_httpClient, options);

            const auto [error, response] = RunSync<CreateBlobContainerResult>(m_context,
                "Create container",
                [&](auto completion)
            {
                m_containerClient->CreateAsync(std::move(completion));
            });
            ASSERT_FALSE(error) << "Failed to create integration test container: " << error.message()
                                << "\nBody: " << response.RawResponse().GetBody();
        }

        void TearDown() override
        {
            if (!m_containerClient)
            {
                return;
            }

            try
            {
                const auto [listError, listResponse] = RunSync<ListBlobsResult>(m_context,
                    "List blobs for cleanup",
                    [&](auto completion)
                {
                    m_containerClient->ListBlobsAsync(std::move(completion));
                });
                if (!listError)
                {
                    for (const auto& blob : listResponse.Value().Blobs)
                    {
                        auto cleanupBlob = m_containerClient->GetBlockBlobClient(blob.Name);
                        [[maybe_unused]] const auto breakResult = RunSync<BreakBlobLeaseResult>(m_context,
                            "Break blob lease for cleanup",
                            [&](auto completion)
                        {
                            cleanupBlob.BreakLeaseAsync(std::move(completion));
                        },
                            std::chrono::seconds{10});

                        AVEVA::AzureClient::DeleteBlobOptions deleteOptions;
                        deleteOptions.DeleteSnapshotsOption = "include";
                        const auto [deleteError, deleteResponse] = RunSync<DeleteBlobResult>(m_context,
                            "Delete blob for cleanup",
                            [&](auto completion)
                        {
                            cleanupBlob.DeleteIfExistsAsync(deleteOptions, std::move(completion));
                        });
                        static_cast<void>(deleteResponse);
                        EXPECT_FALSE(deleteError) << "Failed to delete blob during cleanup: " << blob.Name;
                    }
                }
            }
            catch (const std::exception& ex)
            {
                ADD_FAILURE() << "Cleanup failed before container deletion: " << ex.what();
            }

            try
            {
                const auto [error, response] = RunSync<DeleteBlobContainerResult>(m_context,
                    "Delete integration test container",
                    [&](auto completion)
                {
                    m_containerClient->DeleteAsync(std::move(completion));
                });
                EXPECT_FALSE(error) << "Failed to delete integration test container: " << error.message();
            }
            catch (const std::exception& ex)
            {
                ADD_FAILURE() << "Container cleanup failed: " << ex.what();
            }
        }

        [[nodiscard]] boost::asio::io_context& Context() noexcept
        {
            return m_context;
        }

        [[nodiscard]] BlobContainerClient& Container() noexcept
        {
            return *m_containerClient;
        }

      private:
        boost::asio::io_context m_context;
        std::optional<AVEVA::AzureClient::IntegrationTests::TemporaryCaBundle> m_caBundle;
        std::unique_ptr<IHttpClient> m_httpClient;
        std::unique_ptr<BlobContainerClient> m_containerClient;
    };
} // namespace

TEST_F(AzureBlobIntegrationTest, BlockBlobClient_UploadGetPropertiesAndDelete_RoundTrips)
{
    BlockBlobClient blockBlobClient = Container().GetBlockBlobClient("integration-test-block-blob.txt");
    const std::string content = "Hello from the AVEVA Azure client integration test!";

    const auto [uploadError, uploadResponse] = RunSync<UploadBlockBlobResult>(Context(),
        "Upload block blob",
        [&](auto completion)
    {
        blockBlobClient.UploadAsync(content, std::move(completion));
    });
    ASSERT_FALSE(uploadError) << uploadError.message();
    EXPECT_FALSE(uploadResponse.Value().ETag.empty());

    const auto [propertiesError, propertiesResponse] = RunSync<BlobProperties>(Context(),
        "Get block blob properties",
        [&](auto completion)
    {
        blockBlobClient.GetPropertiesAsync(std::move(completion));
    });
    ASSERT_FALSE(propertiesError) << propertiesError.message();
    EXPECT_EQ(propertiesResponse.Value().Type, BlobType::BlockBlob);
    EXPECT_EQ(propertiesResponse.Value().ContentLength, content.size());

    const auto [deleteError, deleteResponse] = RunSync<DeleteBlobResult>(Context(),
        "Delete block blob",
        [&](auto completion)
    {
        blockBlobClient.DeleteAsync(std::move(completion));
    });
    EXPECT_FALSE(deleteError) << deleteError.message();
}

TEST_F(AzureBlobIntegrationTest, BlockBlobClient_StageAndCommitBlockList_AssemblesBlob)
{
    BlockBlobClient blockBlobClient = Container().GetBlockBlobClient("integration-test-staged-blob.txt");
    const std::string firstChunk = "First chunk. ";
    const std::string secondChunk = "Second chunk.";
    const std::string blockId1 = AVEVA::AzureClient::IntegrationTests::EncodeBlockId("block-0001");
    const std::string blockId2 = AVEVA::AzureClient::IntegrationTests::EncodeBlockId("block-0002");

    const auto [stage1Error, stage1Response] =
        RunStageBlock(Context(), blockBlobClient, "Stage block 1", blockId1, firstChunk);
    ASSERT_FALSE(stage1Error) << stage1Error.message();

    const auto [stage2Error, stage2Response] =
        RunStageBlock(Context(), blockBlobClient, "Stage block 2", blockId2, secondChunk);
    ASSERT_FALSE(stage2Error) << stage2Error.message();

    const auto [commitError, commitResponse] =
        RunCommitBlockList(Context(), blockBlobClient, "Commit block list", {blockId1, blockId2});
    ASSERT_FALSE(commitError) << commitError.message();
    EXPECT_FALSE(commitResponse.Value().ETag.empty());

    const auto [propertiesError, propertiesResponse] =
        RunGetBlobProperties(Context(), blockBlobClient, "Get staged blob properties");
    ASSERT_FALSE(propertiesError) << propertiesError.message();
    EXPECT_EQ(propertiesResponse.Value().ContentLength, firstChunk.size() + secondChunk.size());

    const auto [deleteError, deleteResponse] = RunDeleteBlockBlob(Context(), blockBlobClient, "Delete staged blob");
    EXPECT_FALSE(deleteError) << deleteError.message();
}

TEST_F(AzureBlobIntegrationTest, PageBlobClient_CreateUploadResizeAndDelete_RoundTrips)
{
    PageBlobClient pageBlobClient = Container().GetPageBlobClient("integration-test-page-blob.bin");

    const auto [createError, createResponse] = RunCreatePageBlob(Context(), pageBlobClient, "Create page blob", 1024);
    ASSERT_FALSE(createError) << createError.message();
    EXPECT_FALSE(createResponse.Value().ETag.empty());

    const std::string firstPage(512, 'A');
    const auto [upload1Error, upload1Response] =
        RunUploadPages(Context(), pageBlobClient, "Upload page range 1", 0, firstPage);
    ASSERT_FALSE(upload1Error) << upload1Error.message();

    const std::string secondPage(512, 'B');
    const auto [upload2Error, upload2Response] =
        RunUploadPages(Context(), pageBlobClient, "Upload page range 2", 512, secondPage);
    ASSERT_FALSE(upload2Error) << upload2Error.message();

    const auto [propertiesError, propertiesResponse] =
        RunGetPageBlobProperties(Context(), pageBlobClient, "Get page blob properties");
    ASSERT_FALSE(propertiesError) << propertiesError.message();
    EXPECT_EQ(propertiesResponse.Value().Type, BlobType::PageBlob);
    EXPECT_EQ(propertiesResponse.Value().ContentLength, 1024U);

    const auto [clearError, clearResponse] = RunClearPages(Context(), pageBlobClient, "Clear page range", 0, 512);
    ASSERT_FALSE(clearError) << clearError.message();

    const auto [resizeError, resizeResponse] = RunResizePageBlob(Context(), pageBlobClient, "Resize page blob", 2048);
    ASSERT_FALSE(resizeError) << resizeError.message();

    const auto [resizedPropertiesError, resizedPropertiesResponse] =
        RunGetPageBlobProperties(Context(), pageBlobClient, "Get resized page blob properties");
    ASSERT_FALSE(resizedPropertiesError) << resizedPropertiesError.message();
    EXPECT_EQ(resizedPropertiesResponse.Value().ContentLength, 2048U);

    const auto [deleteError, deleteResponse] = RunDeletePageBlob(Context(), pageBlobClient, "Delete page blob");
    EXPECT_FALSE(deleteError) << deleteError.message();
}
