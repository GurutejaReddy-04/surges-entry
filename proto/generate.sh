#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v protoc &> /dev/null; then
    echo "protoc not found on PATH. Please install protobuf compiler." >&2
    exit 1
fi

if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

mkdir -p gen/event

protoc --go_out=. --go_opt=module=surges-entry/proto --go-grpc_out=. --go-grpc_opt=module=surges-entry/proto event.proto

echo "Successfully generated gRPC stubs in proto/gen/event"
