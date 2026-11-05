// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstdint>

namespace AVEVA::AzureClient {
// Two-valued answers that the client's interfaces give by name instead of as a bare bool, so a call site reads as
// what it decides and an argument cannot be passed the wrong way round. The first enumerator is the negative one.

/// <summary>Whether a text matched what it was compared with.</summary>
enum class StringMatch : std::uint8_t { Mismatch, Match };

/// <summary>Whether a value passed validation.</summary>
enum class Validity : std::uint8_t { Invalid, Valid };

/// <summary>Whether a failed request or error is worth repeating.</summary>
enum class Retriability : std::uint8_t { NotRetriable, Retriable };

/// <summary>Whether an error means a retry can no longer tell if the first attempt took effect.</summary>
enum class DeliveryCertainty : std::uint8_t { Definite, Ambiguous };

/// <summary>Whether an error is expected to go away by itself.</summary>
enum class Transience : std::uint8_t { Permanent, Transient };

/// <summary>Whether a text contains CR, LF or NUL.</summary>
enum class ControlCharacters : std::uint8_t { Absent, Present };

/// <summary>Whether a URL points at this machine.</summary>
enum class HostLocality : std::uint8_t { Remote, Loopback };

/// <summary>Whether an empty value is acceptable where a value is expected.</summary>
enum class EmptyValue : std::uint8_t { Reject, Allow };

/// <summary>Whether a client has a credential of some kind to authorize with.</summary>
enum class CredentialPresence : std::uint8_t { Absent, Present };

/// <summary>Whether a request may be sent again after a failure that leaves its outcome unknown.</summary>
enum class ReplaySafety : std::uint8_t { Unsafe, Safe };

/// <summary>Whether an attempt follows one whose outcome is unknown.</summary>
enum class AttemptKind : std::uint8_t { Initial, AfterAmbiguousFailure };

/// <summary>Whether 404 and 410 responses are retried (the blob may not have replicated yet) or final.</summary>
enum class NotFoundRetry : std::uint8_t { Fail, Retry };

/// <summary>Whether a delete also succeeds when there was nothing to delete.</summary>
enum class MissingTarget : std::uint8_t { Fail, Tolerate };

/// <summary>Whether a failing credential source is the last one the chain will try.</summary>
enum class ChainPosition : std::uint8_t { Intermediate, Last };

/// <summary>Whether a blob exists.</summary>
enum class Existence : std::uint8_t { Absent, Present };

/// <summary>Whether a Content-Range names a range the service could serve ("bytes */total" does not).</summary>
enum class RangeSatisfaction : std::uint8_t { Satisfied, Unsatisfied };

/// <summary>Whether a deletion removed the blob for good (as opposed to a soft delete).</summary>
enum class Permanence : std::uint8_t { Soft, Permanent };

/// <summary>Whether the service stores a blob's content encrypted.</summary>
enum class ServerEncryption : std::uint8_t { Unencrypted, Encrypted };

/// <summary>Whether a listed or fetched blob is the current version.</summary>
enum class VersionCurrency : std::uint8_t { Superseded, Current };

/// <summary>Whether a sink accepted the data handed to it.</summary>
enum class WriteOutcome : std::uint8_t { Failed, Written };

/// <summary>Whether a sink writes on the calling thread or hands the data to another one.</summary>
enum class WriteMode : std::uint8_t { Offloaded, Inline };

/// <summary>Whether a download that fails when written to is stopped or still running.</summary>
enum class Cancellation : std::uint8_t { Running, Canceled };

/// <summary>Whether plain HTTP endpoints are accepted (for local emulators).</summary>
enum class InsecureHttpPolicy : std::uint8_t { Reject, Allow };

/// <summary>Whether a listing includes the blobs' metadata.</summary>
enum class MetadataListing : std::uint8_t { Exclude, Include };

/// <summary>Whether a copy source is authorized with the client's own credential.</summary>
enum class SourceAuthorization : std::uint8_t { None, ClientCredential };

/// <summary>Whether a blob was already gone when a delete ran.</summary>
enum class PriorState : std::uint8_t { Existed, AlreadyAbsent };

/// <summary>Whether an operation (or every operation of a batch) succeeded.</summary>
enum class Completion : std::uint8_t { Incomplete, Complete };

/// <summary>Whether a child request may still start within the aggregate deadline.</summary>
enum class Admission : std::uint8_t { Refused, Admitted };

/// <summary>Whether an aggregate deadline has run out.</summary>
enum class DeadlineState : std::uint8_t { Open, Expired };

/// <summary>Whether a download probe found the blob empty, so that one un-ranged GET must follow.</summary>
enum class FullFetch : std::uint8_t { NotNeeded, Needed };

/// <summary>Whether a size is within the caller's maximum.</summary>
enum class SizeLimit : std::uint8_t { Within, Exceeded };

/// <summary>Whether a download is served in ranges.</summary>
enum class Ranging : std::uint8_t { Unranged, Ranged };

/// <summary>Whether a queued task has begun running.</summary>
enum class TaskProgress : std::uint8_t { Queued, Started };

/// <summary>Whether a request only reads or may modify the blob.</summary>
enum class AccessKind : std::uint8_t { Write, Read };

/// <summary>Whether an attempt that is about to start consumes one of the retry budget's attempts.</summary>
enum class AttemptAccounting : std::uint8_t { Free, Counted };

/// <summary>Whether a request's token has already been refreshed after a 401.</summary>
enum class TokenRefreshState : std::uint8_t { NotRefreshed, Refreshed };

/// <summary>Whether a 401 may be answered with one extra attempt using a freshly requested token.</summary>
enum class RefreshEligibility : std::uint8_t { Ineligible, Eligible };
} // namespace AVEVA::AzureClient
