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

using AVEVA::RocksDB::Plugin::Azure::Plugin;
using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;
using AVEVA::RocksDB::Plugin::Core::ByteCount;

namespace {
ServicePrincipalStorageInfo MakeInfo(const std::string& dbName, const std::string& account = "myaccount") {
    return ServicePrincipalStorageInfo{dbName, "https://" + account + ".blob.core.windows.net", "client", "secret",
                                       "tenant"};
}

// An unusable account URL makes filesystem creation fail fast instead of talking to the network.
ServicePrincipalStorageInfo BadInfo() {
    return ServicePrincipalStorageInfo{"shared-ctx-db", "", "client", "secret", "tenant"};
}

Plugin::RegisterOptions OptionsFor(std::shared_ptr<boost::asio::io_context> context, ServicePrincipalStorageInfo info,
                                   Plugin::Logger logger = nullptr) {
    return Plugin::RegisterOptions{.IoContext = std::move(context),
                                   .Accounts =
                                       Plugin::AccountPair<ServicePrincipalStorageInfo>{std::move(info), std::nullopt},
                                   .Log = std::move(logger)};
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
    auto options = OptionsFor(std::make_shared<boost::asio::io_context>(), BadInfo(), logger);
    options.Cache =
        Plugin::CacheOptions{"cache", ByteCount{static_cast<std::uint64_t>(std::numeric_limits<int64_t>::max()) + 1}};

    const auto status = Plugin::Register(configOptions, &env, &guard, std::move(options));

    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

// A null logger is replaced at registration, so the warning path and every filesystem created afterwards can log.
TEST(PluginTests, Register_WithNullLogger_CreatesUsableEnv) {
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    auto options = OptionsFor(std::make_shared<boost::asio::io_context>(), BadInfo());
    options.Cache = Plugin::CacheOptions{"cache", ByteCount{}};

    const auto status = Plugin::Register(configOptions, &env, &guard, std::move(options));

    EXPECT_FALSE(status.ok());
    Plugin::Unregister(BadInfo());
}

TEST(PluginTests, NameFor_DistinguishesAccountsDatabasesAndBackup) {
    const auto base = Plugin::NameFor(MakeInfo("db"));

    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db2")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db", "other")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db"), MakeInfo("db")));
    EXPECT_EQ(base, Plugin::NameFor(MakeInfo("db")));
}

// The registry name earlier versions used stays published next to the current one.
TEST(PluginTests, Register_PublishesBothTheCurrentAndTheLegacyName) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    // Empty account URLs make filesystem creation fail fast; nothing in this test runs an io_context.
    const auto primary = ServicePrincipalStorageInfo{"legacy-db", "", "client", "secret", "tenant"};
    const auto backup = ServicePrincipalStorageInfo{"legacy-backup", "", "client", "secret", "tenant"};
    auto options = OptionsFor(std::make_shared<boost::asio::io_context>(), primary, logger);
    options.Accounts = Plugin::AccountPair<ServicePrincipalStorageInfo>{primary, backup};

    [[maybe_unused]] const auto status = Plugin::Register(configOptions, &env, &guard, std::move(options));

    auto* library = rocksdb::ObjectLibrary::Default().get();
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(Plugin::NameFor(primary, backup)));
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(std::string(Plugin::Name) + "legacy-dblegacy-backup"));
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
        Plugin::Register(configOptions, &env, &guard, OptionsFor(context, BadInfo(), logger));
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
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    const auto context = std::make_shared<boost::asio::io_context>();
    const auto tooLarge = ByteCount{static_cast<std::uint64_t>(std::numeric_limits<int64_t>::max()) + 1};

    for (const auto badBuffer : {ByteCount{0}, ByteCount{1000}, ByteCount{512}, tooLarge}) {
        auto options = OptionsFor(context, MakeInfo("sizes-db"));
        options.DataFileBufferSize = badBuffer;
        EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, std::move(options)).IsInvalidArgument())
            << badBuffer.Value();
    }
    auto options = OptionsFor(context, MakeInfo("sizes-db"));
    options.DataFileInitialSize = tooLarge;
    EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, std::move(options)).IsInvalidArgument());
    EXPECT_EQ(nullptr, rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::FileSystem>(
                           Plugin::NameFor(MakeInfo("sizes-db"))));
}

TEST(PluginTests, Register_WithNullSharedIoContext_IsRejected) {
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    EXPECT_TRUE(Plugin::Register(configOptions, &env, &guard, OptionsFor(nullptr, MakeInfo("db"))).IsInvalidArgument());
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
    using AVEVA::RocksDB::Plugin::Azure::Registry::ClearRegistrationOwnedBy;
    using AVEVA::RocksDB::Plugin::Azure::Registry::RegistrationOwner;
    using AVEVA::RocksDB::Plugin::Azure::Registry::SetFactory;
    const std::string name = "registry-owner-test";
    const RegistrationOwner first{"first"};
    const RegistrationOwner second{"second"};
    auto calls = std::make_shared<int>(0);
    SetFactory(name, [] { return std::unique_ptr<rocksdb::FileSystem>(); }, first);
    SetFactory(
        name,
        [calls] {
            ++*calls;
            return std::unique_ptr<rocksdb::FileSystem>();
        },
        second);

    ClearRegistrationOwnedBy(name, first);

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    [[maybe_unused]] const auto status = rocksdb::Env::CreateFromUri(configOptions, "", name, &env, &guard);
    EXPECT_EQ(1, *calls);

    ClearRegistrationOwnedBy(name, second);
    [[maybe_unused]] const auto cleared = rocksdb::Env::CreateFromUri(configOptions, "", name, &env, &guard);
    EXPECT_EQ(1, *calls);
}
