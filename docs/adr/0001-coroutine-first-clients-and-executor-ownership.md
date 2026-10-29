# ADR 0001: Coroutine-first clients, strand shards, and executor ownership

- Status: Proposed
- Scope: `libs/HttpClient`, `libs/AzureClient`, the plugin's use of them (`src/Azure`)

## Context

The in-tree HTTP and Azure clients are asynchronous. A review of the `coroutine-only-clients` branch found
that, although the public API is coroutine based, the implementations still contain hand-built callbacks,
waiter vectors, polling cancellation checks, condition-variable style timers, a dedicated sweeper thread and
a five-layer blocking bridge. Much of that exists to be thread-safe with locks and atomics.

The deployment model is fixed:

- The host injects one `io_context` through `Plugin::Register`; it is handed down to the Azure client and the
  HTTP client.
- The host runs that context on N threads. These threads are separate from RocksDB's threads.
- RocksDB threads only call into the plugin synchronously and block on the result.

## Decision

### 1. Coroutines are the only composition mechanism

- Async operations are `awaitable<T>` and compose with `co_await`. Retry loops, fan-out and sequencing are
  straight-line code with local variables, not classes with state flags.
- Cancellation is propagated by asio (cancellation slots, `cancel_after`, awaitable operators). Code does not
  poll `Cancelled()`. A cancelled await throws `operation_aborted`, which is caught once at the operation
  boundary and mapped to `operation_canceled` or a timeout.
- Waiting for an event uses one primitive: a timer set to `time_point::max()` and cancelled on completion, or
  an asio channel. No hand-written waiter vectors or completion-handler lists.
- Bounded parallelism (chunked downloads, batch deletes) uses one shared helper, not per-feature worker pools.

### 2. Strands are the unit of mutual exclusion

- State that must be shared (connection pool, DNS cache, TLS session cache, per-origin gates, token cache)
  lives in a *shard* and is only touched on the shard's `make_strand(injectedExecutor)`.
- Shard state is plain types: no mutexes, atomics or concurrent channels inside it.
- HTTP requests are routed to a shard by hash of `scheme://host:port`, so keep-alive connections, DNS results
  and per-origin limits stay local. An optional sub-shard count lets one hot origin spread across strands.
- `Send` hops onto the shard strand once (inline if already on it) and resumes the caller on its own executor.
- Sockets are created on the shard strand and never cross shards.

### 3. The host owns threads and the io_context

- The clients and the plugin never create threads, call `run()` or call `stop()` on the context.
- Clients take the executor by injection only. The number of shards defaults to a small fixed value and is
  configurable.
- No client-owned background threads. Idle-connection expiry is a timer coroutine per shard, running only
  while its pool is non-empty and holding the shard weakly.

### 4. Blocking happens only at the RocksDB boundary

- The plugin blocks with a single `RunBlocking(executor, awaitable<T>)`, implemented as
  `co_spawn(..., use_future).get()`. Everything below it is `co_await`.
- RocksDB threads are never io threads, so `.get()` cannot deadlock by construction. Calling it on an io
  thread is a contract violation checked with a debug assertion, not defended against at runtime.
- Plugin objects (files) are destroyed on RocksDB threads. Destruction never blocks waiting for io threads to
  make progress; destroying on an io thread is a contract violation.
- Synchronous twins of `*Async` functions are not kept.

### 5. Shutdown is explicit and ordered

`Shutdown()` runs on a non-io thread and:

1. posts a close to every shard strand: set `closed`, cancel in-flight exchanges, clear the pool; pool
   `Release` after close discards the connection;
2. waits, with a timeout, for in-flight coroutines to unwind (per-shard scope counter);
3. only then releases the executor.

The host must keep the context running until `Shutdown()` returns. A timeout is logged as a contract
violation rather than hanging or hiding it.

### 6. Keep the code understandable

- Test seams do not appear in production templates or public headers (no clock/notifier template parameters,
  no test hooks in production classes). Tests drive a controllable executor/clock.
- Implementation lives in `.cpp` files; header templates are limited to thin type-erasure boundaries.
- Prefer a plain container over intrusive structures unless measurement says otherwise.

## Consequences

- Large deletions: the idle sweeper thread and notifier, waiter vectors in the token cache, `OriginGate` and
  `DnsCache`, worker pools in download and batch delete, the five-layer blocking bridge, and polling
  cancellation.
- The correctness argument is "this state is only touched on this strand" rather than lock discipline, which
  is easier to review but must be asserted (`strand.running_in_this_thread()`) in debug builds.
- One extra executor hop per request.
- Per-shard DNS and TLS-session caches duplicate some lookups; accepted.
- A hot single origin is limited to its shard(s) unless sub-sharding is configured.
- The host contract (N threads on one context, kept running until `Shutdown()` returns, RocksDB threads are
  not io threads) must be documented in `Plugin.hpp`.

## Alternatives considered

- **Client-owned io_context and threads.** Rejected: the host already injects the context, and two thread
  pools complicate shutdown.
- **Locks inside pool and caches.** Rejected: correct but harder to reason about, and it keeps the
  waiter/channel machinery.
- **One context per shard supplied by the host.** Possible later; the shard abstraction already supports it,
  but it adds API surface.
- **Round-robin request routing.** Rejected: fragments keep-alive pools and per-origin limits.
