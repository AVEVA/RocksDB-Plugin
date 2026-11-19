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
/// <summary>
/// A download destination, always written in offset order. Sinks are stored by value in the operation, so they must be
/// movable. `Mode` is WriteMode::Inline when Write is cheap and never blocks, so it can run on the I/O thread.
/// </summary>
/// <typeparam name="T">The candidate sink type.</typeparam>
template <class T>
concept DownloadSinkLike = std::movable<T> && requires(T& sink, std::uint64_t size, std::string data) {
    sink.Reserve(size);
    { sink.Write(std::move(data)) } -> std::same_as<WriteOutcome>;
    { T::Mode } -> std::convertible_to<WriteMode>;
};

/// <summary>Writes the download to a file stream the caller has already opened.</summary>
class FileSink final {
  public:
    /// <summary>Takes over the stream the data is written to.</summary>
    /// <param name="file">The open stream to write to.</param>
    explicit FileSink(std::ofstream file) : m_file(std::move(file)) {}

    /// <summary>Writes go to disk, so they are handed off the I/O thread.</summary>
    static constexpr WriteMode Mode = WriteMode::Offloaded;

    /// <summary>Does nothing; the file grows as data is written.</summary>
    /// <param name="size">The total size of the download, unused.</param>
    void Reserve(std::uint64_t /*size*/) {}

    /// <summary>Appends a chunk to the file.</summary>
    /// <param name="data">The next chunk, in offset order.</param>
    /// <returns>Failed when the stream is no longer good after the write.</returns>
    [[nodiscard]] WriteOutcome Write(std::string data) {
        m_file.write(data.data(), gsl::narrow_cast<std::streamsize>(data.size()));
        return m_file.good() ? WriteOutcome::Written : WriteOutcome::Failed;
    }

    /// <summary>Gives access to the stream, for example to flush or close it after the download.</summary>
    /// <returns>The stream, valid for as long as the sink is.</returns>
    [[nodiscard]] std::ofstream& File() noexcept AVEVA_LIFETIMEBOUND { return m_file; }

  private:
    std::ofstream m_file;
};

/// <summary>
/// Copies each received body string into caller-owned memory (one copy; the HTTP client has no body-sink hook
/// to read straight into the span). A blob range larger than the buffer fails the download instead of writing
/// past the end.
/// </summary>
class AVEVA_GSL_POINTER SpanSink final {
  public:
    /// <summary>Wraps the caller's buffer, which must outlive the sink.</summary>
    /// <param name="destination">The memory the download is copied into.</param>
    explicit SpanSink(std::span<char> destination AVEVA_LIFETIME_CAPTURE_BY(this)) : m_destination(destination) {}

    /// <summary>Rejects a download that cannot fit before any data is fetched.</summary>
    /// <param name="size">The total size of the download.</param>
    /// <exception cref="std::length_error">Thrown when <paramref name="size"/> exceeds the buffer.</exception>
    void Reserve(std::uint64_t size) {
        if (size > m_destination.size()) {
            throw std::length_error("The download is larger than the destination buffer.");
        }
    }

    /// <summary>Copying into memory is cheap and never blocks, so it may run on the I/O thread.</summary>
    static constexpr WriteMode Mode = WriteMode::Inline;

    /// <summary>Copies a chunk into the buffer after the data already written.</summary>
    /// <param name="data">The next chunk, in offset order.</param>
    /// <returns>Failed when the chunk would run past the end of the buffer.</returns>
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

/// <summary>Collects the download into a string held in memory.</summary>
class StringSink final {
  public:
    /// <summary>
    /// Upper bound for the up-front reservation. The size comes from the service's response, so it is not trusted to
    /// size an allocation; the string grows geometrically past this as data actually arrives.
    /// </summary>
    static constexpr std::uint64_t MaxReserve = std::uint64_t{64} * 1024U * 1024U;

    /// <summary>Records the expected size, capped at <see cref="MaxReserve"/>, without allocating.</summary>
    /// <param name="size">The total size of the download.</param>
    /// <exception cref="std::length_error">Thrown when <paramref name="size"/> exceeds what a string can
    /// hold.</exception>
    void Reserve(std::uint64_t size) {
        if (size > m_data.max_size()) {
            throw std::length_error("Blob is too large to download into memory.");
        }
        m_expected = std::min(size, MaxReserve);
    }

    /// <summary>Appending to a string is cheap and never blocks, so it may run on the I/O thread.</summary>
    static constexpr WriteMode Mode = WriteMode::Inline;

    /// <summary>
    /// Reserve only records the expected size so that a single chunk covering it is moved in without a copy.
    /// </summary>
    /// <param name="data">The next chunk, in offset order.</param>
    /// <returns>Always Written; growing the string reports failure by throwing instead.</returns>
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

    /// <summary>Hands out the collected data, leaving the sink empty.</summary>
    /// <returns>The data written so far.</returns>
    [[nodiscard]] std::string Take() noexcept { return std::move(m_data); }

  private:
    std::string m_data;
    std::uint64_t m_expected = 0;
};
} // namespace AVEVA::AzureClient::Private
