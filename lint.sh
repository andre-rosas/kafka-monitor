#!/bin/bash

# Kafka Monitor - Lint Script
# Checks code formatting and style

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }
print_info() { echo -e "${YELLOW}ℹ${NC} $1"; }

SERVICES=("shared" "order-processor" "query-processor" "registry-processor" "monitor")
FAILED=0
FIX_MODE=${1:-"check"}  # "check" or "fix"

for service in "${SERVICES[@]}"; do
    if [ -d "$service" ]; then
        print_header "Linting $service"
        cd "$service"
        
        # 1. Check formatting (cljfmt)
        if [ "$FIX_MODE" == "fix" ]; then
            print_info "Fixing formatting..."
            lein cljfmt fix
            print_success "Formatting fixed"
        else
            print_info "Checking formatting..."
            if lein cljfmt check; then
                print_success "Formatting OK"
            else
                print_error "Formatting issues found"
                echo "Run: ./lint.sh fix"
                FAILED=$((FAILED + 1))
            fi
        fi
        
        # 2. Style suggestions (kibit)
        print_info "Running Kibit (style suggestions)..."
        if lein kibit; then
            print_success "No style suggestions"
        else
            print_info "Style suggestions found (not critical)"
        fi
        
        # 3. Static analysis (eastwood)
        print_info "Running Eastwood (static analysis)..."
        if lein eastwood; then
            print_success "Static analysis passed"
        else
            print_error "Static analysis issues found"
            FAILED=$((FAILED + 1))
        fi
        
        # 4. Code smell detection (bikeshed)
        print_info "Running Bikeshed (code smells)..."
        if lein bikeshed; then
            print_success "No code smells"
        else
            print_info "Code smell warnings (review recommended)"
        fi
        
        cd ..
    fi
done

echo ""
print_header "Summary"

if [ $FAILED -eq 0 ]; then
    print_success "All linting checks passed!"
    exit 0
else
    print_error "$FAILED service(s) failed linting"
    exit 1
fi