#!/bin/bash
echo "COMMIT_SHA=$(git rev-parse HEAD)" > "functions/.env"
