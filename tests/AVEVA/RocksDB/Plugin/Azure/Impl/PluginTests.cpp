// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "PluginRegistry.hpp"

#include <gtest/gtest.h>
#include <rocksdb/convenience.h>
#include <rocksdb/file_system.h>
#include <rocksdb/utilities/object_registry.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>

#include <cctype>
#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>

#if defined(_MSC_VER)
#define DEPRECATED_CALL_BEGIN __pragma(warning(push)) __pragma(warning(disable : 4996))
#define DEPRECATED_CALL_END __pragma(warning(pop))
#else
#define DEPRECATED_CALL_BEGIN                                                                                          \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define DEPRECATED_CALL_END _Pragma("GCC diagnostic pop")
#endif

using AVEVA::RocksDB::Plugin::Azure::Plugin;
using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

namespace {
ServicePrincipalStorageInfo MakeInfo(const std::string& dbName, const std::string& account = "myaccount") {
    return ServicePrincipalStorageInfo{dbName, "https://" + account + ".blob.core.windows.net", "client", "secret",
                                       "tenant"};
}

// An unusable account URL makes filesystem creation fail fast instead of talking to the network.
ServicePrincipalStorageInfo BadInfo() {
    return ServicePrincipalStorageInfo{"shared-ctx-db", "", "client", "secret", "tenant"};
}
} // namespace

TEST(PluginTests, NameFor_StartsWithPluginNameAndIsHexEncoded) {
    const auto name = Plugin::NameFor(MakeInfo("container"));

    ASSERT_EQ(0U, name.find(std::string(Plugin::Name) + "-"));
    for (const char ch : name.substr(Plugin::Name.size() + 1)) {
        EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(ch))) << ch;
    }
}

TEST(PluginTests, Register_WithCacheSizeAboveInt64Max_IsRejected) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    boost::asio::io_context context;

    const auto status = Plugin::Register(configOptions, &env, &guard, context, BadInfo(), std::nullopt, logger, 4096,
                                         4096, "cache", static_cast<size_t>(std::numeric_limits<int64_t>::max()) + 1);

    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(PluginTests, NameFor_DistinguishesAccountsDatabasesAndBackup) {
    const auto base = Plugin::NameFor(MakeInfo("db"));

    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db2")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db", "other")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db"), MakeInfo("db")));
    EXPECT_EQ(base, Plugin::NameFor(MakeInfo("db")));
}

// The pre-io_context overload still compiles, owns its io_context, and publishes the old registry name too.
TEST(PluginTests, DeprecatedRegister_WithoutIoContext_PublishesBothNamesAndShutsDownCleanly) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    {
        DEPRECATED_CALL_BEGIN
        [[maybe_unused]] const auto status =
            Plugin::Register(configOptions, &env, &guard, MakeInfo("legacy-db"), MakeInfo("legacy-backup"), logger);
        DEPRECATED_CALL_END
    }

    auto* library = rocksdb::ObjectLibrary::Default().get();
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(
                           Plugin::NameFor(MakeInfo("legacy-db"), MakeInfo("legacy-backup"))));
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(std::string(Plugin::Name) + "legacy-dblegacy-backup"));
    // Destroying the Env must not hang or touch a stopped io_context.
    guard.reset();
}

TEST(PluginTests, Register_WithSharedIoContext_KeepsItAliveUntilUnregistered) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    auto context = std::make_shared<boost::asio::io_context>();
    const std::weak_ptr<boost::asio::io_context> weak = context;

    [[maybe_unused]] const auto status =
        Plugin::Register(configOptions, &env, &guard, context, BadInfo(), std::nullopt, logger);
    guard.reset();
    EXPECT_GT(context.use_count(), 1);

    Plugin::Unregister(BadInfo());
    EXPECT_EQ(1, context.use_count());

    rocksdb::Env* again = nullptr;
    std::shared_ptr<rocksdb::Env> againGuard;
    const auto created =
        rocksdb::Env::CreateFromUri(configOptions, "", Plugin::NameFor(BadInfo()), &again, &againGuard);
    EXPECT_FALSE(created.ok());
}

TEST(PluginTests, Register_WithInvalidDataFileSizes_IsRejected) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    boost::asio::io_context context;
    constexpr int64_t kGoodBuffer = 4096;

    for (const int64_t badBuffer : {int64_t{0}, int64_t{-512}, int64_t{1000}, int64_t{512}}) {
        EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, context, MakeInfo("sizes-db"), std::nullopt, logger,
                                     badBuffer)
                        .IsInvalidArgument())
            << badBuffer;
    }
    EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, context, MakeInfo("sizes-db"), std::nullopt, logger,
                                 kGoodBuffer, -1)
                    .IsInvalidArgument());
    EXPECT_EQ(nullptr, rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::FileSystem>(
                           Plugin::NameFor(MakeInfo("sizes-db"))));
}

TEST(PluginTests, Register_WithNullSharedIoContext_IsRejected) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, std::shared_ptr<boost::asio::io_context>{},
                                 MakeInfo("db"), std::nullopt, logger)
                    .IsInvalidArgument());
}
namespace {
// Reports, from its own destructor, whether the registration lock was free: a helper thread clears the same
// registration and can only finish if nobody is holding the lock while the destructor runs.
struct LockProbe {
    std::string Name;
    std::shared_ptr<bool> LockWasFree;
    LockProbe(std::string name, std::shared_ptr<bool> result) : Name(std::move(name)), LockWasFree(std::move(result)) {}
    LockProbe(LockProbe&&) noexcept = default;
    LockProbe(const LockProbe&) = default;
    ~LockProbe() {
        if (LockWasFree) {
            auto cleared = std::async(std::launch::async, [name = Name] {
                AVEVA::RocksDB::Plugin::Azure::Registry::ClearRegistration(name);
            });
            *LockWasFree = cleared.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
        }
    }
};
} // namespace

TEST(PluginRegistryTests, SetFactory_ReplacingAFactory_DestroysTheOldOneOutsideTheRegistrationLock) {
    using AVEVA::RocksDB::Plugin::Azure::Registry::ClearRegistration;
    using AVEVA::RocksDB::Plugin::Azure::Registry::FileSystemFactory;
    using AVEVA::RocksDB::Plugin::Azure::Registry::SetFactory;
    const std::string name = "registry-lock-test";
    auto lockWasFree = std::make_shared<bool>(false);
    SetFactory(name, [probe = LockProbe(name, lockWasFree)] { return std::unique_ptr<rocksdb::FileSystem>(); });

    SetFactory(name, FileSystemFactory{});

    EXPECT_TRUE(*lockWasFree);
    ClearRegistration(name);
}
TEST(PluginRegistryTests, ClearRegistration_WithAnotherOwner_KeepsTheSharedName) {
    using AVEVA::RocksDB::Plugin::Azure::Registry::ClearRegistration;
    using AVEVA::RocksDB::Plugin::Azure::Registry::SetFactory;
    const std::string name = "registry-owner-test";
    const std::string first = "first";
    const std::string second = "second";
    auto calls = std::make_shared<int>(0);
    SetFactory(name, [] { return std::unique_ptr<rocksdb::FileSystem>(); }, first);
    SetFactory(
        name,
        [calls] {
            ++*calls;
            return std::unique_ptr<rocksdb::FileSystem>();
        },
        second);

    ClearRegistration(name, &first);

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    [[maybe_unused]] const auto status = rocksdb::Env::CreateFromUri(configOptions, "", name, &env, &guard);
    EXPECT_EQ(1, *calls);

    ClearRegistration(name, &second);
    [[maybe_unused]] const auto cleared = rocksdb::Env::CreateFromUri(configOptions, "", name, &env, &guard);
    EXPECT_EQ(1, *calls);
}
