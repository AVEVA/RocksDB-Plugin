// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

// Wraps the few test call sites that deliberately exercise [[deprecated]] overloads (warnings are errors).
#if defined(_MSC_VER) && !defined(__clang__)
#define AVEVA_TEST_ALLOW_DEPRECATED_BEGIN __pragma(warning(push)) __pragma(warning(disable : 4996))
#define AVEVA_TEST_ALLOW_DEPRECATED_END __pragma(warning(pop))
#else
#define AVEVA_TEST_ALLOW_DEPRECATED_BEGIN                                                                              \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define AVEVA_TEST_ALLOW_DEPRECATED_END _Pragma("GCC diagnostic pop")
#endif
