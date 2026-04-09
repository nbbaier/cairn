#!/usr/bin/env bash
set -euo pipefail

# Ensure dependencies are up to date
cargo check --workspace 2>/dev/null || cargo fetch
