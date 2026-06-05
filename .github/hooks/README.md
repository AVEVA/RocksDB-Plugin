# Hooks

This folder contains repository hook scripts in a GitHub-oriented layout.

## Pre-commit Hook

The `pre-commit` script scans staged files for secrets and formats staged C/C++ files.

To use it locally on Linux/macOS/Git Bash:

```bash
cp .github/hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

On Windows PowerShell:

```powershell
Copy-Item .github/hooks/pre-commit .git/hooks/pre-commit -Force
```
