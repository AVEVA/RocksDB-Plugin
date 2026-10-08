# Third-party notices

> They have not been reviewed by AVEVA legal / OSS compliance and must be confirmed before release.

Dependencies come from the root `vcpkg.json`. Verify exact versions and license texts with `vcpkg install` output
(`share/<port>/copyright` in the installed tree).

| Dependency | Used for | License |
|---|---|---|
| aveva-http-client | HTTP transport | Apache License 2.0 (vendored in libs/HttpClient) |
| Boost (asio, url, uuid, algorithm, json; beast for tests) | Async model, parsing | Boost Software License 1.0 |
| Azure SDK for C++ (portions of the culture-ordering tables and comparator in `src/SharedKeySigner.cpp`) | Shared Key canonicalization | MIT, Copyright (c) Microsoft Corporation |
| OpenSSL | TLS and HMAC/SHA primitives | Apache License 2.0 (OpenSSL 3.x) |
| libxml2 | XML response parsing | MIT |
| GoogleTest (tests only) | Unit tests | BSD 3-Clause |
| bemanproject/exemplar (root `infrastructure/cmake/aveva-install-library-config.cmake`, derived) | CMake install helper | Apache License 2.0 with LLVM exception |

Binary redistribution of this library together with its dependencies must reproduce each dependency's
copyright and license notice. The library itself is licensed under Apache-2.0 (see `LICENSE` in this directory).
