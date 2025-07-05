#!/bin/bash

# Script to check Docker and Docker Compose versions

echo "================================================================"
echo "Docker Environment Check"
echo "================================================================"
echo ""

# Check if Docker is installed
echo "1. Checking Docker installation..."
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version)
    echo "   ✓ Docker is installed: $DOCKER_VERSION"
    
    # Check Docker daemon
    if docker info &> /dev/null; then
        echo "   ✓ Docker daemon is running"
    else
        echo "   ✗ Docker daemon is not running or not accessible"
        echo "     Try: sudo systemctl start docker (Linux) or start Docker Desktop"
    fi
else
    echo "   ✗ Docker is not installed"
    echo "     Install from: https://docs.docker.com/get-docker/"
    exit 1
fi

echo ""
echo "2. Checking Docker Compose V2..."
if docker compose version &> /dev/null; then
    COMPOSE_V2_VERSION=$(docker compose version)
    echo "   ✓ Docker Compose V2 is available: $COMPOSE_V2_VERSION"
else
    echo "   ✗ Docker Compose V2 is not available"
    echo ""
    echo "   Checking for Docker Compose V1..."
    if command -v docker-compose &> /dev/null; then
        COMPOSE_V1_VERSION=$(docker-compose --version)
        echo "   ⚠ Docker Compose V1 found: $COMPOSE_V1_VERSION"
        echo ""
        echo "   To use this setup with V1, you'll need to:"
        echo "   - Replace 'docker compose' with 'docker-compose' in all scripts"
        echo "   - Or upgrade to Docker Compose V2"
    else
        echo "   ✗ No Docker Compose version found"
    fi
    echo ""
    echo "   To install Docker Compose V2:"
    echo "   - Docker Desktop: Already included"
    echo "   - Linux: sudo apt-get install docker-compose-plugin (Debian/Ubuntu)"
    echo "           sudo yum install docker-compose-plugin (RHEL/CentOS)"
fi

echo ""
echo "3. Checking Docker version compatibility..."
# Extract Docker version number
DOCKER_VERSION_NUM=$(docker --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
MAJOR_VERSION=$(echo $DOCKER_VERSION_NUM | cut -d. -f1)
MINOR_VERSION=$(echo $DOCKER_VERSION_NUM | cut -d. -f2)

if [ "$MAJOR_VERSION" -gt 20 ] || ([ "$MAJOR_VERSION" -eq 20 ] && [ "$MINOR_VERSION" -ge 10 ]); then
    echo "   ✓ Docker version $DOCKER_VERSION_NUM is compatible with Compose V2"
else
    echo "   ⚠ Docker version $DOCKER_VERSION_NUM is older than recommended"
    echo "     Compose V2 requires Docker Engine 20.10.0 or later"
fi

echo ""
echo "================================================================"

# Exit with appropriate code
if docker compose version &> /dev/null; then
    echo "✅ Your system is ready to use the Fund ETL Docker setup!"
    exit 0
else
    echo "⚠️  Please install or upgrade Docker Compose to use this setup."
    exit 1
fi
