// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Tests {
using TestLogger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

// A non-null logger for tests that need one but do not inspect its output.
inline std::shared_ptr<TestLogger> MakeTestLogger() { return std::make_shared<TestLogger>(); }
} // namespace AVEVA::RocksDB::Plugin::Tests
