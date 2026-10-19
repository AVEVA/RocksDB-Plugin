// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "PluginRegistry.hpp"
#include <rocksdb/utilities/object_registry.h>

#include <boost/log/trivial.hpp>

#include <mutex>
#include <unordered_map>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Registry {
namespace {
/// <summary>
/// The settings of the latest registration for a plugin name. The RocksDB factory reads this state instead of
/// capturing the settings; otherwise registering again (for example with a new io_context after the previous one
/// was destroyed) would keep using the old settings.
/// </summary>
struct Registration {
    std::mutex Mutex;
    FileSystemFactory Create;
};

struct RegistrationTable {
    std::mutex Mutex;
    std::unordered_map<std::string, std::shared_ptr<Registration>> Registrations;
};

RegistrationTable& Table() {
    static RegistrationTable table;
    return table;
}

/// <summary>
/// Returns the registration state for `pluginName`, adding the RocksDB factory that uses it on first call.
/// </summary>
std::shared_ptr<Registration> GetOrAddRegistration(const std::string& pluginName) {
    std::scoped_lock lock(Table().Mutex);
    auto& registration = Table().Registrations[pluginName];
    if (!registration) {
        registration = std::make_shared<Registration>();
        rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::FileSystem>(
            pluginName,
            [registration](const std::string& /* uri */, std::unique_ptr<rocksdb::FileSystem>* f, std::string* errmsg) {
                FileSystemFactory create;
                {
                    std::scoped_lock registrationLock(registration->Mutex);
                    create = registration->Create;
                }

                if (!create) {
                    if (errmsg != nullptr) {
                        *errmsg = "Azure blob filesystem plugin has not been registered yet";
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                }

                // RocksDB's C-style callers cannot handle exceptions escaping a factory.
                try {
                    *f = create();
                } catch (const std::exception& ex) {
                    if (errmsg != nullptr) {
                        *errmsg = std::string("Failed to create Azure blob filesystem: ") + ex.what();
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                } catch (...) {
                    if (errmsg != nullptr) {
                        *errmsg = "Failed to create Azure blob filesystem: unknown error";
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                }
                return f->get();
            });
    }

    return registration;
}
} // namespace

void SetFactory(const std::string& pluginName, FileSystemFactory create) {
    auto registration = GetOrAddRegistration(pluginName);
    // Declared before the lock so the old factory is destroyed after unlocking: it may own an io_context whose
    // destructor joins a thread.
    FileSystemFactory replaced;
    std::scoped_lock lock(registration->Mutex);
    replaced = std::exchange(registration->Create, std::move(create));
}

void ClearRegistration(const std::string& pluginName) {
    std::shared_ptr<Registration> registration;
    {
        std::scoped_lock lock(Table().Mutex);
        if (const auto it = Table().Registrations.find(pluginName); it != Table().Registrations.end()) {
            registration = it->second;
        }
    }
    if (registration) {
        // Destroy the old factory outside the lock: releasing an owned io_context may join its thread.
        FileSystemFactory released;
        std::scoped_lock lock(registration->Mutex);
        released = std::move(registration->Create);
        registration->Create = nullptr;
    }
}

// A handler that throws would otherwise end run() and kill the worker silently, leaving every later operation on
// this shared context to hang. Keep serving until the work guard is released and the queue drains.
OwnedIoContext::OwnedIoContext()
    : m_thread([this] {
          for (;;) {
              try {
                  m_context.run();
                  return;
              } catch (const std::exception& ex) {
                  BOOST_LOG_TRIVIAL(error) << "An io_context handler threw; continuing: " << ex.what();
              } catch (...) {
                  BOOST_LOG_TRIVIAL(error) << "An io_context handler threw a non-standard exception; continuing";
              }
          }
      }) {}

OwnedIoContext::~OwnedIoContext() {
    m_workGuard.reset();
    m_thread.join();
}

std::shared_ptr<OwnedIoContext> OwnedIoContext::Acquire() {
    static std::mutex mutex;
    static std::weak_ptr<OwnedIoContext> current;
    std::scoped_lock lock(mutex);
    auto shared = current.lock();
    if (!shared) {
        shared = std::make_shared<OwnedIoContext>();
        current = shared;
    }
    return shared;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Registry
