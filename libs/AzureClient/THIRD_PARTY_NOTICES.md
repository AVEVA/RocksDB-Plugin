# Third-party notices

> DRAFT: license identifiers below are the upstream licenses as commonly published for the vcpkg ports.
> They have not been reviewed by AVEVA legal / OSS compliance and must be confirmed before release.

Dependencies come from `vcpkg.json`. Verify exact versions and license texts with `vcpkg install` output
(`share/<port>/copyright` in the installed tree).

| Dependency | Used for | License |
|---|---|---|
| aveva-http-client | HTTP transport | AVEVA internal (see that repository) |
| Boost (asio, url, uuid, algorithm, property-tree; beast for tests) | Async model, parsing | Boost Software License 1.0 |
| OpenSSL | TLS and HMAC/SHA primitives | Apache License 2.0 (OpenSSL 3.x) |
| libxml2 | XML response parsing | MIT |
| GoogleTest (tests only) | Unit tests | BSD 3-Clause |
| Google Benchmark (benchmarks only) | Micro-benchmarks | Apache License 2.0 |

Binary redistribution of this library together with its dependencies must reproduce each dependency's
copyright and license notice. Whether packages may be redistributed outside AVEVA Group is a legal
decision; the repository `LICENSE` currently restricts distribution to internal use.
