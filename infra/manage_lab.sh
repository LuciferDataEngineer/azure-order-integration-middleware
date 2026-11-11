#!/usr/bin/env bash
# Manual control for a specific authorized project resource group and Azure DevOps environment.
set -euo pipefail
case "${1:-}" in plan|deploy) action=$1 ;; *) echo 'Choose plan or deploy.' >&2; exit 2 ;; esac
case "${ORDERS_PHASE:-}" in foundation|runtime|wired) ;; *) echo 'Choose an approved deployment phase.' >&2; exit 2 ;; esac
for key in ORDERS_APPROVAL_REF ORDERS_RESOURCE_GROUP ORDERS_SQL_ADMIN_NAME ORDERS_SQL_ADMIN_ID; do
  [[ -n "${!key:-}" && "${!key}" != UNCONFIGURED && "${!key}" != UNAPPROVED ]] || { echo "Missing reviewed value for ${key}." >&2; exit 2; }
done
uuid='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ "$ORDERS_SQL_ADMIN_ID" =~ $uuid ]] || { echo 'Invalid SQL administrator group ID.' >&2; exit 2; }
app_id=${ORDERS_APPLICATION_CLIENT_ID:-}
if [[ "$ORDERS_PHASE" != foundation && ! "$app_id" =~ $uuid ]]; then
  echo 'Supply the approved Functions audience application client ID.' >&2; exit 2
fi
image=${ORDERS_EXPORT_IMAGE:-}
if [[ -n "$image" && ! "$image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]]; then
  echo 'Pin the approved public exporter image to its SHA-256 digest.' >&2; exit 2
fi
args=(
  --name orders-lab --mode Incremental
  --resource-group "$ORDERS_RESOURCE_GROUP"
  --template-file "$(dirname "$0")/main.bicep"
  --parameters environment=dev location=australiaeast phase="$ORDERS_PHASE"
  sqlAdminName="$ORDERS_SQL_ADMIN_NAME" sqlAdminObjectId="$ORDERS_SQL_ADMIN_ID"
  applicationClientId="$app_id" exporterImage="$image" actionGroupId="${ORDERS_ACTION_GROUP:-}"
)
if [[ "$action" == plan ]]; then az deployment group what-if "${args[@]}"; else az deployment group create "${args[@]}"; fi
