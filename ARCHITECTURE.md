# Architecture Overview

## Purpose

The AVEVA RocksDB Plugin provides filesystem and caching extensions so RocksDB can run in stateless cloud deployments backed by Azure Blob Storage.

## Components

1. `src/Azure/`
   - Azure Page Blob filesystem implementation.
   - Azure client (`libs/AzureClient` / `libs/HttpClient`) integration, credential handling, and status translation.
2. `src/Core/`
   - Shared filesystem abstractions and utility logic.
   - Secondary cache, file cache, and related indexing helpers.
3. `include/AVEVA/RocksDB/Plugin/`
   - Public API surface for plugin consumers.
4. `tests/`
   - Unit and integration tests validating plugin behavior.

## Client Libraries

- `libs/HttpClient` (`aveva-http-client`): asio/Beast HTTP client with DNS cache, connection pool, TLS session cache and
  an idempotency-aware retry policy. The transport does not enforce a scheme policy; `AllowInsecureHttp` is enforced by
  `libs/AzureClient`.
- `libs/AzureClient` (`aveva-azure-client`): Blob Storage REST client built on HttpClient. Transport failures are mapped
  to `BlobStorageErrorCode`, with the transport cause kept in the error details.

## Threading and Ownership

The plugin never owns or runs an `io_context`; the host injects it (`Plugin::Register`). Blocking calls use
`co_spawn` + `use_future` + `.get()` (see `Impl/RequestHelpers.hpp`) and must never run on a thread servicing that `io_context`. Destroy plugin objects before
stopping the host `io_context`.

## File Semantics

- `Flush` hands data to the page-blob writer; `Sync` makes it durable. Reads on a read/write file are bounded by the
  last `Sync`.
- Page blobs are page-aligned; the logical size is stored in blob metadata.
- Rename is copy + delete, not atomic. A crash in between can leave both blobs.
- Writers are fenced by blob leases that a renewal loop keeps alive; losing the lease fails further writes.

## File cache

`Core::FileCache` keeps local copies of SST files (other file types are never cached). It is enabled per primary
container by passing `cachePath` and `maxCacheSize` to `Plugin::Register`.

- **Two-access admission.** The first read of an SST only records a metadata entry (stale, size 0). The second read
  queues a download on the cache's background thread; reads are served from the blob until the download finishes and
  the entry becomes active. This stops a startup scan from downloading every SST once.
- **LRU.** Entries sit in a list ordered by last access. Before a download is admitted, entries are evicted from the
  least recently used end until the new file fits; entries that are downloading or queued are skipped, and a file
  larger than `maxCacheSize` is not cached.
- **Invalidation.** Writing to or replacing a blob marks its entry stale and bumps a generation counter, so a
  download that raced with the change is discarded rather than activated.
- **Background deletes.** Evicted or removed entries leave the accounting immediately, but their read handles are
  closed and files deleted on the background thread, outside the cache mutex (an open file cannot be deleted on
  Windows). A failed delete is logged, retried on every background wake-up, and tried once more at shutdown.
- **Stale-entry cap.** Metadata-only stale entries are capped at 2048; beyond that the oldest are pruned in O(1) from a
  dedicated list so the index cannot grow without bound.

## Breaking changes (0.2.0)

See the migration section in `README.md`.

## Key Design Expectations

1. Public headers remain stable and avoid leaking implementation details.
2. Azure client failures (`RequestFailedException`) are translated into meaningful RocksDB `Status` values.
3. Cache and filesystem components are thread-safe and deterministic.
4. Changes to Azure or cache behavior are covered by tests.
