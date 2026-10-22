// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

// Lifetime annotations (https://clang.llvm.org/docs/LifetimeSafety.html). They are compiled out on toolchains that
// do not understand them, so they are a contract for the analysis and never change behaviour.
//
//   AVEVA_LIFETIMEBOUND        The returned value (or constructed object) refers to the annotated parameter, or to
//                              `*this` when written after a member function's declarator. Using it after that
//                              object dies is a dangling reference.
//   AVEVA_LIFETIME_CAPTURE_BY  The annotated parameter is stored in the named object (a parameter name or `this`),
//                              so the parameter must outlive that object.
//   AVEVA_NOESCAPE             The callee only uses the annotated pointer/reference parameter during the call.
//   AVEVA_GSL_POINTER          The type is a non-owning view (like std::string_view); it must not outlive its source.
//   AVEVA_GSL_OWNER            The type owns what it refers to (like std::unique_ptr).
#if defined(__clang__) && defined(__has_cpp_attribute)
#if __has_cpp_attribute(clang::lifetimebound)
#define AVEVA_LIFETIMEBOUND [[clang::lifetimebound]]
#endif
#if __has_cpp_attribute(clang::lifetime_capture_by)
#define AVEVA_LIFETIME_CAPTURE_BY(owner) [[clang::lifetime_capture_by(owner)]]
#endif
#if __has_cpp_attribute(clang::noescape)
#define AVEVA_NOESCAPE [[clang::noescape]]
#endif
#elif defined(_MSC_VER) && defined(__has_cpp_attribute)
#if __has_cpp_attribute(msvc::lifetimebound)
#define AVEVA_LIFETIMEBOUND [[msvc::lifetimebound]]
#endif
#endif

#ifndef AVEVA_LIFETIMEBOUND
#define AVEVA_LIFETIMEBOUND
#endif
#ifndef AVEVA_LIFETIME_CAPTURE_BY
#define AVEVA_LIFETIME_CAPTURE_BY(owner)
#endif
#ifndef AVEVA_NOESCAPE
#define AVEVA_NOESCAPE
#endif

#if defined(__clang__) && defined(__has_cpp_attribute)
#if __has_cpp_attribute(gsl::Pointer)
#define AVEVA_GSL_POINTER [[gsl::Pointer]]
#define AVEVA_GSL_OWNER [[gsl::Owner]]
#endif
#endif
#ifndef AVEVA_GSL_POINTER
#define AVEVA_GSL_POINTER
#define AVEVA_GSL_OWNER
#endif
