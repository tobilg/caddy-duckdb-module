#!/usr/bin/env bash
#
# Development setup script for Caddy DuckDB Extension
#
# This script:
# 1. Checks prerequisites (Go, C compiler)
# 2. Creates required directories
# 3. Downloads dependencies
# 4. Builds binaries
# 5. Generates an initial API key
#

set -euo pipefail

# Configuration
DATA_DIR="${DATA_DIR:-/tmp/data}"
AUTH_DB="${DATA_DIR}/auth.db"
MIN_GO_VERSION="1.24"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

success() {
    echo -e "${GREEN}✓${NC} $1"
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

die() {
    error "$1"
    exit 1
}

# Print banner
print_banner() {
    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     Caddy DuckDB Extension - Development Setup    ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Check Go version
check_go() {
    info "Checking Go installation..."

    if ! command -v go &> /dev/null; then
        die "Go is not installed. Please install Go ${MIN_GO_VERSION} or later."
    fi

    GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
    GO_MAJOR=$(echo "$GO_VERSION" | cut -d. -f1)
    GO_MINOR=$(echo "$GO_VERSION" | cut -d. -f2)

    MIN_MAJOR=$(echo "$MIN_GO_VERSION" | cut -d. -f1)
    MIN_MINOR=$(echo "$MIN_GO_VERSION" | cut -d. -f2)

    if [ "$GO_MAJOR" -lt "$MIN_MAJOR" ] || ([ "$GO_MAJOR" -eq "$MIN_MAJOR" ] && [ "$GO_MINOR" -lt "$MIN_MINOR" ]); then
        die "Go version ${GO_VERSION} is too old. Please install Go ${MIN_GO_VERSION} or later."
    fi

    success "Go ${GO_VERSION} found"
}

# Check C compiler
check_cc() {
    info "Checking C compiler..."

    if command -v gcc &> /dev/null; then
        CC_VERSION=$(gcc --version | head -n1)
        success "GCC found: ${CC_VERSION}"
    elif command -v clang &> /dev/null; then
        CC_VERSION=$(clang --version | head -n1)
        success "Clang found: ${CC_VERSION}"
    elif command -v cc &> /dev/null; then
        success "C compiler found (cc)"
    else
        die "No C compiler found. Please install gcc or clang."
    fi
}

# Check platform
check_platform() {
    info "Checking platform..."

    case "$(uname -s)" in
        Darwin)
            success "Platform: macOS"
            ;;
        Linux)
            success "Platform: Linux"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            warn "Platform: Windows (limited support)"
            warn "Consider using WSL2 for better compatibility"
            ;;
        *)
            warn "Platform: Unknown ($(uname -s))"
            ;;
    esac
}

# Create directories
create_dirs() {
    info "Creating data directories..."

    if [ -d "$DATA_DIR" ]; then
        success "Data directory already exists: ${DATA_DIR}"
    else
        mkdir -p "$DATA_DIR"
        success "Created data directory: ${DATA_DIR}"
    fi
}

# Download dependencies
download_deps() {
    info "Downloading Go dependencies..."
    go mod download
    success "Dependencies downloaded"
}

# Build binaries
build_binaries() {
    info "Building Caddy binary..."
    CGO_ENABLED=1 go build -o caddy ./cmd/caddy
    success "Built: caddy"

    info "Building API key tool..."
    CGO_ENABLED=1 go build -o tools/create-api-key ./tools/create-api-key.go
    success "Built: tools/create-api-key"
}

# Generate API key
generate_api_key() {
    info "Generating admin API key..."

    # Run the tool and capture output
    API_KEY_OUTPUT=$(./tools/create-api-key -db "$AUTH_DB" -role admin 2>&1)

    # Extract the API key from output
    API_KEY=$(echo "$API_KEY_OUTPUT" | grep "API Key:" | awk '{print $3}')

    if [ -n "$API_KEY" ]; then
        # Save API key to file for reference
        echo "$API_KEY" > "${DATA_DIR}/.api-key"
        chmod 600 "${DATA_DIR}/.api-key"
        success "API key generated and saved to ${DATA_DIR}/.api-key"
    else
        warn "Could not extract API key from output"
        echo "$API_KEY_OUTPUT"
    fi
}

# Print summary
print_summary() {
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}                  Setup Complete!                   ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo ""

    if [ -f "${DATA_DIR}/.api-key" ]; then
        API_KEY=$(cat "${DATA_DIR}/.api-key")
        echo -e "Your API Key: ${YELLOW}${API_KEY}${NC}"
        echo ""
    fi

    echo "Next steps:"
    echo ""
    echo "  1. Start the server:"
    echo -e "     ${BLUE}make run${NC}"
    echo ""
    echo "  2. Test the API:"
    if [ -f "${DATA_DIR}/.api-key" ]; then
        echo -e "     ${BLUE}curl -H \"X-API-Key: ${API_KEY}\" http://localhost:8080/duckdb/query -d '{\"sql\": \"SELECT 1\"}'${NC}"
    else
        echo -e "     ${BLUE}curl -H \"X-API-Key: YOUR_KEY\" http://localhost:8080/duckdb/query -d '{\"sql\": \"SELECT 1\"}'${NC}"
    fi
    echo ""
    echo "  3. View available commands:"
    echo -e "     ${BLUE}make help${NC}"
    echo ""
}

# Main
main() {
    print_banner

    # Change to project root (script is in scripts/)
    cd "$(dirname "$0")/.."

    echo "Step 1/6: Checking prerequisites"
    echo "─────────────────────────────────"
    check_platform
    check_go
    check_cc
    echo ""

    echo "Step 2/6: Creating directories"
    echo "─────────────────────────────────"
    create_dirs
    echo ""

    echo "Step 3/6: Downloading dependencies"
    echo "─────────────────────────────────"
    download_deps
    echo ""

    echo "Step 4/6: Building binaries"
    echo "─────────────────────────────────"
    build_binaries
    echo ""

    echo "Step 5/6: Generating API key"
    echo "─────────────────────────────────"
    generate_api_key
    echo ""

    echo "Step 6/6: Finalizing"
    echo "─────────────────────────────────"
    success "Setup complete!"

    print_summary
}

main "$@"
