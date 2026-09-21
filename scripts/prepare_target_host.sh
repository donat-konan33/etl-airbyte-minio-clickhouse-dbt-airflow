#!/usr/bin/env bash
set -euo pipefail

SUDO=""
if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  SUDO="sudo"
fi

DEPLOY_PATH="${DEPLOY_PATH:-/opt/etl-stack}"
REQUIRED_REMOTE_USER="${REQUIRED_REMOTE_USER:-${USER:-}}"

log() {
  echo "[prepare-target-host] $*"
}

require_authorized_user() {
  current_user="$(id -un 2>/dev/null || echo "unknown")"

  if [[ -n "${REQUIRED_REMOTE_USER}" ]] && [[ "${current_user}" != "${REQUIRED_REMOTE_USER}" ]]; then
    echo "Deployment is restricted to the authorized remote user '${REQUIRED_REMOTE_USER}', but this session is running as '${current_user}'." >&2
    exit 1
  fi

  log "Authorized deployment user check passed for '${current_user}'."
}

require_root_for_install() {
  if [[ -n "${SUDO}" ]]; then
    log "The script will use sudo for package installation."
  fi
}

install_prereqs_apt() {
  log "Updating package lists..."
  ${SUDO} apt-get update

  log "Installing base tools (git, ca-certificates, curl, gnupg)..."
  ${SUDO} apt-get install -y ca-certificates curl gnupg git

  if ! command -v docker >/dev/null 2>&1; then
    log "Installing Docker Engine..."
    ${SUDO} install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | ${SUDO} gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    ${SUDO} chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      ${SUDO} tee /etc/apt/sources.list.d/docker.list >/dev/null

    ${SUDO} apt-get update
    ${SUDO} apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi

  if ! docker compose version >/dev/null 2>&1; then
    log "Installing Docker Compose plugin..."
    ${SUDO} apt-get install -y docker-compose-plugin
  fi
}

ensure_git() {
  if ! command -v git >/dev/null 2>&1; then
    log "git is missing. Installing git..."
    ${SUDO} apt-get install -y git
  fi
}

ensure_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    log "docker is missing. Installing Docker..."
    install_prereqs_apt
  fi

  if ! docker compose version >/dev/null 2>&1; then
    log "docker compose is missing. Installing Compose plugin..."
    ${SUDO} apt-get install -y docker-compose-plugin
  fi
}

ensure_docker_daemon() {
  if ! docker info >/dev/null 2>&1; then
    log "Docker daemon is not running. Starting it..."
    ${SUDO} systemctl enable --now docker || true
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "Docker is installed but the daemon is not reachable." >&2
    echo "Please start Docker manually: sudo systemctl enable --now docker" >&2
    exit 1
  fi
}

ensure_user_in_docker_group() {
  if [[ -n "${SUDO}" ]]; then
    current_user="$(id -un)"
    if ! groups | grep -q docker; then
      log "Adding current user to the docker group..."
      ${SUDO} usermod -aG docker "${current_user}"
      log "Docker group updated. Log out and log back in before rerunning this check."
    fi
  fi
}

main() {
  require_authorized_user
  require_root_for_install

  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    case "${ID:-}" in
      ubuntu|debian)
        ;;
      *)
        echo "This installer currently supports Ubuntu/Debian hosts only." >&2
        exit 1
        ;;
    esac
  else
    echo "Unable to detect the operating system." >&2
    exit 1
  fi

  ensure_git
  ensure_docker
  ensure_docker_daemon
  ensure_user_in_docker_group

  if ! id -nG "$(id -un)" | grep -qw docker; then
    echo "The current user '$(id -un)' is not allowed to use Docker. Add the user to the docker group or deploy as the authorized user." >&2
    exit 1
  fi

  if ! command -v git >/dev/null 2>&1; then
    echo "git is still unavailable after installation." >&2
    exit 1
  fi

  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is still unavailable after installation." >&2
    exit 1
  fi

  if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is still unavailable after installation." >&2
    exit 1
  fi

  mkdir -p "${DEPLOY_PATH}"
  log "Target directory ready: ${DEPLOY_PATH}"

  log "Preflight checks passed. Required tools are installed and the Docker daemon is reachable."
  log "Verified commands:"
  command -v git
  command -v docker
  docker compose version
}

main "$@"
