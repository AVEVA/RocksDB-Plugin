// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/RetryOptions.hpp"

#include <random>

namespace AVEVA {
std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber) {
    thread_local std::mt19937_64 generator{std::random_device{}()};
    return ComputeBackoff(retry, retryNumber, generator);
}
} // namespace AVEVA
