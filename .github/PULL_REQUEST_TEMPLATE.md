<!--
=== INSTRUCTIONS (Delete this entire block before submitting) ===

PR Title Format: <type>(scope): subject
  - type: feat, fix, docs, refactor, perf, test, build, chore
  - Breaking change? Add ! after scope: feat(scope)!: description
  - Subject: lowercase, imperative verb, no period

Before completing:
  - Customize merge commit message (remove "Merged PR #####: " text)
  - Delete source branch
  - Review test output from pipeline

=== END INSTRUCTIONS ===
-->

UserStory Number: N/A

## PR Type

Select the conventional commit type used in the PR title (`<type>(<scope>): <subject>`):

- [ ] feat - New feature
- [ ] fix - Bug fix
- [ ] docs - Documentation changes
- [ ] style - Formatting/style-only changes (no logic change)
- [ ] refactor - Refactoring (no functional change)
- [ ] perf - Performance improvement
- [ ] test - Tests added/updated
- [ ] build - Build system/dependencies/tooling
- [ ] ci - CI/CD pipeline changes
- [ ] chore - Maintenance/housekeeping
- [ ] revert - Revert previous change

Release note: use `release` as scope in the title (for example: `chore(release): prepare 1.2.3` or `build(release): update package metadata`).

## Does this PR introduce a breaking change?

- [ ] Yes
- [ ] No

<!-- A breaking change includes any change to a public header in include/AVEVA/RocksDB/Plugin/. If this PR contains a breaking change, please describe the impact and migration path for existing applications below. -->

## What is the current behavior?
<!-- Please describe the current behavior that you are modifying, or link to a relevant issue. -->


## What is the new behavior?


## How was this verified?
<!-- e.g. CMake preset built, ctest suites that ran and passed. -->


## Other information


---

## For Reviewers

### Best Practices
Please review the [Contributing guidelines](../CONTRIBUTING.md) before requesting review.

### Security Compliance

> The items below follow public secure-coding guidance from [OWASP](https://owasp.org/) and the [Microsoft C++ security docs](https://learn.microsoft.com/en-us/cpp/security/). Reviewers should manually verify the relevant items for the changes in this PR.

- [ ] Avoid banned or unsafe APIs ([SDL banned function calls](https://learn.microsoft.com/en-us/previous-versions/bb288454(v=msdn.10)); prefer the [security-enhanced CRT functions](https://learn.microsoft.com/en-us/cpp/c-runtime-library/security-enhanced-versions-of-crt-functions)).
- [ ] Validate and sanitize all untrusted input, including file/path parameters (guard against directory traversal via `..\` / `/`).
- [ ] Guard against out-of-bounds access on STL containers; check bounds before indexing.
- [ ] Catch specific exceptions where possible; reserve generic exception handling as a last resort.
- [ ] Null out freed pointers to reduce the severity of double-free / dangling-pointer bugs.
- [ ] Enable compiler hardening switches where applicable (`/GS`, `/guard:cf`, `/NXCOMPAT`, `/DYNAMICBASE`).
- [ ] Data-at-rest is evaluated for sensitivity and encrypted where needed; data-in-transit is always encrypted.
- [ ] Use built-in/system libraries for cryptography; no custom or easily reversible algorithms. Use strong key sizes (AES-256+, RSA-2048+).

