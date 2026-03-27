#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-niru:local}"
CONTAINER_NAME="${CONTAINER_NAME:-niru}"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.yaml}"
SERVICE_ACCOUNT_FILE="${SERVICE_ACCOUNT_FILE:-$ROOT_DIR/service-account.json}"
CONTAINER_SERVICE_ACCOUNT_FILE="/run/secrets/google-service-account.json"
DOCKER_CONTEXT="${DOCKER_CONTEXT:-$(docker context show)}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/docker.sh build
  ./scripts/docker.sh once [--no-build] [--config-in-image] [--service-account-json] [--detach]
  ./scripts/docker.sh loop [--no-build] [--config-in-image] [--service-account-json] [--detach]
  ./scripts/docker.sh run [--mode once|loop] [--no-build] [--config-in-image] [--service-account-json] [--detach]

Options:
  --image NAME             Override the image tag (default: niru:local)
  --name NAME              Override the container name (default: niru)
  --env-file PATH          Path to the env file passed to docker run
  --config PATH            Path to a config file to mount at /app/config.yaml
  --service-account PATH   Path to a service-account JSON file to mount
  --mode once|loop         Runtime mode for the run command
  --no-build               Skip docker build before running
  --config-in-image        Use the config baked into the image instead of a bind mount
  --service-account-json   Use GOOGLE_SERVICE_ACCOUNT_JSON instead of a file mount
  --detach                 Run the container in the background
  --attach                 Run the container in the foreground
  --restart POLICY         Docker restart policy for run/loop (default: unless-stopped for loop, no for once)
  --help                   Show this help text

Anything after `--` is passed through to `docker run`.
EOF
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    echo "Missing $label: $path" >&2
    exit 1
  fi
}

run_docker() {
  local stderr_file
  local status
  local api_version

  stderr_file="$(mktemp)"
  if docker "$@" 2> >(tee "$stderr_file" >&2); then
    rm -f "$stderr_file"
    return 0
  fi

  status=$?
  api_version="$(
    sed -nE 's/.*Maximum supported API version is ([0-9.]+).*/\1/p' "$stderr_file" \
      | tail -n 1
  )"
  rm -f "$stderr_file"

  if [[ -n "$api_version" ]]; then
    echo "Retrying docker $1 with DOCKER_API_VERSION=$api_version" >&2
    DOCKER_API_VERSION="$api_version" docker "$@"
    return $?
  fi

  return "$status"
}

command="${1:-run}"
if [[ $# -gt 0 ]]; then
  shift
fi

mode="once"
do_build=1
config_strategy="auto"
service_account_strategy="auto"
detach=0
restart_policy=""
extra_docker_args=()

case "$command" in
  build)
    ;;
  once)
    mode="once"
    ;;
  loop)
    mode="loop"
    detach=1
    restart_policy="unless-stopped"
    ;;
  run)
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage
    exit 1
    ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      IMAGE_NAME="$2"
      shift 2
      ;;
    --name)
      CONTAINER_NAME="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --service-account)
      SERVICE_ACCOUNT_FILE="$2"
      shift 2
      ;;
    --mode)
      mode="$2"
      shift 2
      ;;
    --no-build)
      do_build=0
      shift
      ;;
    --config-in-image)
      config_strategy="image"
      shift
      ;;
    --service-account-json)
      service_account_strategy="json"
      shift
      ;;
    --detach)
      detach=1
      shift
      ;;
    --attach)
      detach=0
      shift
      ;;
    --restart)
      restart_policy="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      extra_docker_args=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$command" == "build" || "$do_build" -eq 1 ]]; then
  run_docker build -t "$IMAGE_NAME" "$ROOT_DIR"
fi

if [[ "$command" == "build" ]]; then
  exit 0
fi

if [[ "$mode" != "once" && "$mode" != "loop" ]]; then
  echo "Invalid mode: $mode" >&2
  exit 1
fi

require_file "$ENV_FILE" "env file"

is_remote_context=0
case "$DOCKER_CONTEXT" in
  default|desktop-linux)
    ;;
  *)
    is_remote_context=1
    ;;
esac

if [[ "$config_strategy" == "auto" ]]; then
  if [[ "$is_remote_context" -eq 1 ]]; then
    config_strategy="image"
  else
    config_strategy="mount"
  fi
fi

if [[ "$service_account_strategy" == "auto" ]]; then
  if [[ "$is_remote_context" -eq 1 ]]; then
    service_account_strategy="json"
  else
    service_account_strategy="mount"
  fi
fi

docker_args=(
  run
  --name "$CONTAINER_NAME"
  --env-file "$ENV_FILE"
)

if [[ "$detach" -eq 1 ]]; then
  docker_args+=(--detach)
fi

if [[ -n "$restart_policy" && "$restart_policy" != "no" ]]; then
  docker_args+=(--restart "$restart_policy")
else
  docker_args+=(--rm)
fi

if [[ "$config_strategy" == "mount" ]]; then
  require_file "$CONFIG_FILE" "config file"
  docker_args+=(-v "$CONFIG_FILE:/app/config.yaml:ro")
fi

if [[ "$service_account_strategy" == "mount" ]]; then
  require_file "$SERVICE_ACCOUNT_FILE" "service account file"
  docker_args+=(
    -e "GOOGLE_SERVICE_ACCOUNT_FILE=$CONTAINER_SERVICE_ACCOUNT_FILE"
    -e "GOOGLE_SERVICE_ACCOUNT_JSON="
    -v "$SERVICE_ACCOUNT_FILE:$CONTAINER_SERVICE_ACCOUNT_FILE:ro"
  )
elif [[ "$service_account_strategy" == "json" ]]; then
  if [[ -f "$SERVICE_ACCOUNT_FILE" ]]; then
    service_account_json="$(tr -d '\r\n' < "$SERVICE_ACCOUNT_FILE")"
    docker_args+=(
      -e "GOOGLE_SERVICE_ACCOUNT_FILE="
      -e "GOOGLE_SERVICE_ACCOUNT_JSON=$service_account_json"
    )
  fi
fi

if [[ ${#extra_docker_args[@]} -gt 0 ]]; then
  docker_args+=("${extra_docker_args[@]}")
fi

docker_args+=("$IMAGE_NAME" python main.py --mode "$mode")

run_docker "${docker_args[@]}"
