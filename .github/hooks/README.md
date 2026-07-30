# Hooks

This folder contains repository Git hook scripts. Git does not run them automatically
just because they live here — `core.hooksPath` is a per-clone setting that each contributor
must configure once (see [Setup](#setup)). Keeping the scripts in the repo lets them be
versioned and shared across the team without copying into `.git/hooks`.

## Pre-commit Hook

The `pre-commit` script:

- Scans staged files for secrets with [gitleaks](https://github.com/gitleaks/gitleaks)
  (required — the commit is blocked if gitleaks is not installed).
- Formats staged C/C++ files under `src/`, `include/`, and `tests/` with `clang-format`.

## Setup

Run the environment doctor from the repository root — it configures the hooks path
automatically and reports any missing tools:

```powershell
./devdoctor.ps1
```

To configure the hooks path manually:

```powershell
git config --local core.hooksPath .github/hooks
```

This is a one-time setup per clone. Once `core.hooksPath` is configured, the `pre-commit`
hook runs automatically on every commit in this repository.

