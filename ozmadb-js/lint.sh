#!/usr/bin/env bash
# Lint the application.

set -ex

yarn lint --no-fix --max-warnings 0
