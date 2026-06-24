# Hooks

This folder contains repository Git hook scripts. The repository is configured to
use this directory as the hooks path (`core.hooksPath = .github/hooks`) so hooks are
versioned and shared across the team — no manual copying into `.git/hooks` is needed.

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

