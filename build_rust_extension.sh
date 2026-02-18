#!/bin/bash
# Build script for the Rust InMemoryCore PyO3 extension

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'  # No Color

echo -e "${BLUE}Building Rust InMemoryCore extension...${NC}"

# Check prerequisites
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: Rust toolchain not found. Install from https://rustup.rs/${NC}"
    exit 1
fi

if ! command -v python &> /dev/null; then
    echo -e "${RED}Error: Python not found${NC}"
    exit 1
fi

# Detect Python version
PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}Using Python ${PYTHON_VERSION}${NC}"

# Check for uvx (used to run maturin)
if ! command -v uvx &> /dev/null; then
    echo -e "${RED}Error: uvx not found. Install uv from https://github.com/astral-sh/uv${NC}"
    exit 1
fi

# Build
echo -e "${BLUE}Building extension in release mode...${NC}"
cd "$(dirname "$0")"

if [ "$1" == "--wheel" ]; then
    # Build wheel
    echo -e "${BLUE}Creating redistributable wheel...${NC}"
    uvx maturin build --release
    echo -e "${GREEN}Wheel built in target/wheels/${NC}"
    ls target/wheels/
else
    # Development build (installs in current venv)
    echo -e "${BLUE}Installing extension for development...${NC}"
    uvx maturin develop --release
fi

echo -e "${GREEN}Build complete!${NC}"

# Verify
echo -e "${BLUE}Verifying extension loads...${NC}"
python -c "from pgqueuer.core_rs import InMemoryCore; print('✓ InMemoryCore loaded successfully')"

# Optional: Run tests
if [ "$2" == "--test" ]; then
    echo -e "${BLUE}Running tests...${NC}"
    pytest test/test_port_conformance.py -v
fi

echo -e "${GREEN}All done!${NC}"
