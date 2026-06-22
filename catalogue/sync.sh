#!/usr/bin/env bash
# catalogue-sync.sh — copy this into any project that imports the catalogue via git subtree.
#
# Usage:
#   ./scripts/catalogue-sync.sh pull    # pull latest from catalogue repo
#   ./scripts/catalogue-sync.sh push    # push your local catalogue edits back
#
# One-off setup (run once per project, not needed again):
#   git remote add catalogue git@github.com:dantelore/data-catalogue.git
#   git subtree add --prefix=catalogue catalogue main --squash

set -euo pipefail

REMOTE="catalogue"
PREFIX="catalogue"
BRANCH="main"

cmd="${1:-}"

case "$cmd" in
  pull)
    echo "Pulling catalogue updates from $REMOTE/$BRANCH into $PREFIX/..."
    git subtree pull --prefix="$PREFIX" "$REMOTE" "$BRANCH" --squash
    echo "Done."
    ;;
  push)
    echo "Pushing $PREFIX/ changes back to $REMOTE/$BRANCH..."
    git subtree push --prefix="$PREFIX" "$REMOTE" "$BRANCH"
    echo "Done."
    ;;
  *)
    echo "Usage: $0 pull|push"
    echo ""
    echo "  pull  — fetch latest catalogue entries from the catalogue repo"
    echo "  push  — push any catalogue edits you've made back to the catalogue repo"
    echo ""
    echo "One-off setup (if not already done):"
    echo "  git remote add $REMOTE git@github.com:dantelore/data-catalogue.git"
    echo "  git subtree add --prefix=$PREFIX $REMOTE $BRANCH --squash"
    exit 1
    ;;
esac
