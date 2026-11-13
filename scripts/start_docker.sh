#!/usr/bin/env bash
# ==============================================================================
# start_docker.sh - Automated Docker Daemon Startup & Readiness Check
# ==============================================================================
set -euo pipefail

TIMEOUT_SECONDS=90
POLL_INTERVAL=2

log() {
    echo -e "\033[1;34m[DOCKER-START]\033[0m $*"
}

success() {
    echo -e "\033[1;32m[DOCKER-START] ✓\033[0m $*"
}

warn() {
    echo -e "\033[1;33m[DOCKER-START] ⚠\033[0m $*"
}

error() {
    echo -e "\033[1;31m[DOCKER-START] ✗\033[0m $*" >&2
}

# 1. Verify Docker CLI exists
if ! command -v docker >/dev/null 2>&1; then
    error "Docker CLI is not found in PATH. Please install Docker first."
    exit 1
fi

# 2. Check if Docker daemon is already accessible
if docker info >/dev/null 2>&1; then
    SERVER_VER=$(docker info --format '{{.ServerVersion}}' 2>/dev/null || echo "Unknown")
    success "Docker daemon is already running and accessible! (Server Version: ${SERVER_VER})"
    exit 0
fi

log "Docker daemon is not currently responding. Attempting to start Docker..."

# 3. Platform-specific startup
OS_TYPE="$(uname -s)"
case "${OS_TYPE}" in
    Darwin*)
        if [ -d "/Applications/Docker.app" ]; then
            log "Launching Docker Desktop (/Applications/Docker.app)..."
            open -a "/Applications/Docker.app"
        elif [ -d "/Applications/OrbStack.app" ]; then
            log "Launching OrbStack (/Applications/OrbStack.app)..."
            open -a "/Applications/OrbStack.app"
        else
            error "No supported Docker application found in /Applications (looked for Docker.app and OrbStack.app)."
            exit 1
        fi
        ;;
    Linux*)
        if command -v systemctl >/dev/null 2>&1; then
            log "Starting Docker service via systemctl..."
            sudo systemctl start docker || true
        elif command -v service >/dev/null 2>&1; then
            log "Starting Docker service via service..."
            sudo service docker start || true
        else
            warn "Unable to detect service manager. Please start the Docker daemon manually."
        fi
        ;;
    *)
        warn "Unsupported OS (${OS_TYPE}). Please launch Docker manually."
        ;;
esac

# 4. Wait for daemon readiness
log "Waiting for Docker daemon to become responsive (timeout: ${TIMEOUT_SECONDS}s)..."

ELAPSED=0
while [ "${ELAPSED}" -lt "${TIMEOUT_SECONDS}" ]; do
    if docker info >/dev/null 2>&1; then
        SERVER_VER=$(docker info --format '{{.ServerVersion}}' 2>/dev/null || echo "Unknown")
        success "Docker daemon is ready! (Server Version: ${SERVER_VER})"
        exit 0
    fi
    sleep "${POLL_INTERVAL}"
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
    echo -n "."
done

echo ""
error "Timed out after ${TIMEOUT_SECONDS}s waiting for Docker daemon to respond."
error "Please verify Docker Desktop is running and that your user has permissions to connect to the Docker socket."
exit 1
