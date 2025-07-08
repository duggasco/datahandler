#!/bin/bash
#
# Fund ETL Test Suite Runner
# Executes comprehensive tests for the ETL system
#

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Fund ETL Test Suite${NC}"
echo "=================="
echo ""

# Function to run tests in Docker
run_docker_tests() {
    echo -e "${YELLOW}Running tests in Docker container...${NC}"
    
    # Make test runner executable
    docker compose exec fund-etl chmod +x /app/tests/run_tests.py
    
    # Run the tests
    docker compose exec fund-etl python /app/tests/run_tests.py "$@"
    
    # Copy test results to host
    echo ""
    echo -e "${YELLOW}Copying test results to host...${NC}"
    docker compose cp fund-etl:/app/tests/test_report.txt ./tests/
    docker compose cp fund-etl:/app/tests/test_results.json ./tests/
    
    echo -e "${GREEN}Test results saved to ./tests/${NC}"
}

# Function to run specific test module
run_module() {
    local module=$1
    echo -e "${YELLOW}Running test module: $module${NC}"
    docker compose exec fund-etl python -m unittest tests.${module} -v
}

# Function to run quick tests (no Docker rebuild)
run_quick() {
    echo -e "${YELLOW}Running quick tests (no rebuild)...${NC}"
    run_docker_tests --failfast
}

# Function to run with coverage
run_coverage() {
    echo -e "${YELLOW}Running tests with coverage...${NC}"
    docker compose exec fund-etl coverage run -m unittest discover tests -v
    docker compose exec fund-etl coverage report
    docker compose exec fund-etl coverage html
    echo -e "${GREEN}Coverage report generated in htmlcov/${NC}"
}

# Parse command line arguments
case "$1" in
    "")
        # Default: run all tests
        run_docker_tests
        ;;
    "quick")
        # Quick test run
        run_quick
        ;;
    "module")
        # Run specific module
        if [ -z "$2" ]; then
            echo -e "${RED}Error: Module name required${NC}"
            echo "Usage: $0 module <module_name>"
            exit 1
        fi
        run_module "$2"
        ;;
    "coverage")
        # Run with coverage
        run_coverage
        ;;
    "verbose")
        # Run with extra verbosity
        run_docker_tests --verbose 3
        ;;
    "help"|"-h"|"--help")
        # Show help
        echo "Usage: $0 [command] [options]"
        echo ""
        echo "Commands:"
        echo "  (none)     Run all tests"
        echo "  quick      Run tests with failfast enabled"
        echo "  module     Run specific test module"
        echo "  coverage   Run tests with coverage report"
        echo "  verbose    Run tests with extra verbosity"
        echo "  help       Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                    # Run all tests"
        echo "  $0 quick              # Quick test run"
        echo "  $0 module test_database  # Run database tests only"
        echo "  $0 coverage           # Generate coverage report"
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac