// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <optional>
#include <stdexcept>

namespace AVEVA::AzureClient::Tests
{
    // Returns a reference to the value held by `opt`, throwing (which gtest reports as a test
    // failure) when it is empty.
    //
    // Tests use this instead of `*opt` or `opt.value()` because bugprone-unchecked-optional-access
    // has to *prove* that an access cannot fail, and it cannot see through the early return hidden
    // inside `ASSERT_TRUE(opt.has_value())`. Routing the access through an explicitly guarded helper
    // keeps the assertion diagnosable and the access provably checked.
    template <class T> [[nodiscard]] const T& ValueOrFail(const std::optional<T>& opt)
    {
        if (!opt.has_value())
        {
            throw std::logic_error{"expected an engaged std::optional"};
        }
        return *opt;
    }

    template <class T> [[nodiscard]] T& ValueOrFail(std::optional<T>& opt)
    {
        if (!opt.has_value())
        {
            throw std::logic_error{"expected an engaged std::optional"};
        }
        return *opt;
    }
} // namespace AVEVA::AzureClient::Tests
