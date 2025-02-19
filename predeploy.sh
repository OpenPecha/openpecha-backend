#!/bin/bash
PROJECT_ROOT=$(git rev-parse --show-toplevel)
echo "COMMIT_SHA=$(git rev-parse HEAD)" > "$PROJECT_ROOT/functions/.env"
