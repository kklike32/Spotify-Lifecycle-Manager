#!/bin/bash
# Install git hooks for this repository
# Run this once after cloning: ./scripts/hooks/install-hooks.sh

REPO_ROOT="$(git rev-parse --show-toplevel)"
HOOKS_DIR="$REPO_ROOT/.git/hooks"
SCRIPTS_HOOKS_DIR="$REPO_ROOT/scripts/hooks"

echo "Installing git hooks..."

# Install pre-commit hook
if [ -f "$SCRIPTS_HOOKS_DIR/pre-commit" ]; then
    cp "$SCRIPTS_HOOKS_DIR/pre-commit" "$HOOKS_DIR/pre-commit"
    chmod +x "$HOOKS_DIR/pre-commit"
    echo "✓ Installed pre-commit hook"
else
    echo "✗ pre-commit hook not found at $SCRIPTS_HOOKS_DIR/pre-commit"
    exit 1
fi

echo ""
echo "Git hooks installed successfully!"
echo ""
echo "The pre-commit hook will now run 'make check' before every commit."
echo "To bypass the check (not recommended): git commit --no-verify"
