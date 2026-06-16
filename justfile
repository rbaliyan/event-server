# Install tools via mise
setup:
    mise install

# Generate protobuf code
proto:
    protoc \
        --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        --grpc-gateway_out=. --grpc-gateway_opt=paths=source_relative,generate_unbound_methods=true \
        -I proto \
        -I $(go env GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway/v2@v2.25.1 \
        -I $(go env GOPATH)/pkg/mod/github.com/googleapis/googleapis@v0.0.0-20250115164207-1a7da9e5054f \
        proto/event/v1/event.proto

# Download googleapis for proto imports
proto-deps:
    @mkdir -p third_party/googleapis/google/api
    @curl -sL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o third_party/googleapis/google/api/annotations.proto
    @curl -sL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o third_party/googleapis/google/api/http.proto

# Generate proto with local third_party (outputs to proto dir)
proto-local:
    protoc \
        --go_out=proto --go_opt=paths=source_relative \
        --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
        --grpc-gateway_out=proto --grpc-gateway_opt=paths=source_relative,generate_unbound_methods=true \
        -I proto \
        -I third_party/googleapis \
        proto/event/v1/event.proto

# Build and install the eventctl binary to GOBIN
install:
    go install ./cmd/eventctl/...

# Run tests
test:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Run integration tests against a throwaway Redis (build tag `integration`).
# Auto-detects the container runtime (prefers docker, falls back to podman) and
# skips if neither is installed. Redis is published on host port 6399 to avoid
# colliding with a local Redis on 6379.
test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    runtime=""
    for r in docker podman; do
        if command -v "$r" >/dev/null 2>&1; then runtime="$r"; break; fi
    done
    if [ -z "$runtime" ]; then
        echo "no container runtime (docker or podman) found; skipping integration tests"
        exit 0
    fi
    echo "using $runtime"
    cid=$("$runtime" run -d --rm -p 127.0.0.1:6399:6379 redis:7-alpine)
    trap '"$runtime" rm -f "$cid" >/dev/null 2>&1 || true' EXIT
    for _ in $(seq 1 30); do
        if "$runtime" exec "$cid" redis-cli ping >/dev/null 2>&1; then break; fi
        sleep 1
    done
    EVENT_REDIS_ADDR=127.0.0.1:6399 go test -tags integration -race ./...

# Build all packages
build:
    go build ./...

# Tidy dependencies
tidy:
    go mod tidy

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    #!/usr/bin/env bash
    set -euo pipefail
    latest=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
    major=$(echo "$latest" | cut -d. -f1)
    minor=$(echo "$latest" | cut -d. -f2)
    patch=$(echo "$latest" | cut -d. -f3)
    next="${major}.${minor}.$((patch + 1))"
    echo "Tagging ${next} (was ${latest})"
    git tag -a "${next}" -m "Release ${next}"
    git push origin "${next}"
