// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <gsl/util>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <ios>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace AVEVA::AzureClient::Private {
// A download destination, always written in offset order. Sinks are stored by value in the operation, so they must be
// movable. `Mode` is WriteMode::Inline when Write is cheap and never blocks, so it can run on the I/O thread.
template <class T>
concept DownloadSinkLike = std::movable<T> && requires(T& sink, std::uint64_t size, std::string data) {
    sink.Reserve(size);
    { sink.Write(std::move(data)) } -> std::same_as<WriteOutcome>;
    { T::Mode } -> std::convertible_to<WriteMode>;
};

class FileSink final {
  public:
    explicit FileSink(std::ofstream file) : m_file(std::move(file)) {}

    static constexpr WriteMode Mode = WriteMode::Offloaded;

    void Reserve(std::uint64_t /*size*/) {}

    [[nodiscard]] WriteOutcome Write(std::string data) {
        m_file.write(data.data(), gsl::narrow_cast<std::streamsize>(data.size()));
        return m_file.good() ? WriteOutcome::Written : WriteOutcome::Failed;
    }

    [[nodiscard]] std::ofstream& File() noexcept AVEVA_LIFETIMEBOUND { return m_file; }

  private:
    std::ofstream m_file;
};

// Copies each received body string into caller-owned memory (one copy; the HTTP client has no body-sink hook
// to read straight into the span). A blob range larger than the buffer fails the download instead of writing
// past the end.
class AVEVA_GSL_POINTER SpanSink final {
  public:
    explicit SpanSink(std::span<char> destination AVEVA_LIFETIME_CAPTURE_BY(this)) : m_destination(destination) {}

    void Reserve(std::uint64_t size) {
        if (size > m_destination.size()) {
            throw std::length_error("The download is larger than the destination buffer.");
        }
    }

    static constexpr WriteMode Mode = WriteMode::Inline;

    [[nodiscard]] WriteOutcome Write(std::string data) {
        if (data.size() > m_destination.size() - m_position) {
            return WriteOutcome::Failed;
        }
        std::ranges::copy(data, m_destination.begin() + gsl::narrow_cast<std::ptrdiff_t>(m_position));
        m_position += data.size();
        return WriteOutcome::Written;
    }

  private:
    std::span<char> m_destination;
    std::size_t m_position = 0;
};

class StringSink final {
  public:
    // Upper bound for the up-front reservation. The size comes from the service's response, so it is not trusted to
    // size an allocation; the string grows geometrically past this as data actually arrives.
    static constexpr std::uint64_t MaxReserve = std::uint64_t{64} * 1024U * 1024U;

    void Reserve(std::uint64_t size) {
        if (size > m_data.max_size()) {
            throw std::length_error("Blob is too large to download into memory.");
        }
        m_expected = std::min(size, MaxReserve);
    }

    static constexpr WriteMode Mode = WriteMode::Inline;

    // Reserve only records the expected size so that a single chunk covering it is moved in without a copy.
    [[nodiscard]] WriteOutcome Write(std::string data) {
        if (m_data.empty() && data.size() >= m_expected) {
            m_data = std::move(data);
            return WriteOutcome::Written;
        }
        if (m_data.empty()) {
            m_data.reserve(gsl::narrow_cast<std::size_t>(m_expected));
        }
        m_data.append(data);
        return WriteOutcome::Written;
    }

    [[nodiscard]] std::string Take() noexcept { return std::move(m_data); }

  private:
    std::string m_data;
    std::uint64_t m_expected = 0;
};
} // namespace AVEVA::AzureClient::Private
