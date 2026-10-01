#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

if [ ! -f .env ]; then
	printf 'Arquivo .env não encontrado. Crie-o com: cp .env.example .env\n' >&2
	exit 1
fi

set -a
. ./.env
set +a

scenario=${1:-duplicate}
shift 2>/dev/null || true

case "$scenario" in
	duplicate)
		set -- -e "VUS=${K6_DUPLICATE_VUS:-50}" -e "ITERATIONS=${K6_DUPLICATE_ITERATIONS:-50}" "$@"
		;;
	compete)
		;;
	parallel)
		set -- -e "VUS=${K6_PARALLEL_VUS:-10}" "$@"
		;;
	*)
		printf 'Cenário inválido: %s (use duplicate, compete ou parallel)\n' "$scenario" >&2
		exit 2
		;;
esac

exec k6 run \
	-e "SCENARIO=$scenario" \
	-e "API_BASE_URL=$API_BASE_URL" \
	-e "OIDC_ISSUER_URL=$OIDC_ISSUER_URL" \
	-e "OIDC_PROVIDER_CLIENT_ID=$OIDC_PROVIDER_CLIENT_ID" \
	-e "OIDC_PROVIDER_CLIENT_SECRET=$OIDC_PROVIDER_CLIENT_SECRET" \
	-e "OIDC_INTERNAL_CLIENT_ID=$OIDC_INTERNAL_CLIENT_ID" \
	-e "OIDC_INTERNAL_CLIENT_SECRET=$OIDC_INTERNAL_CLIENT_SECRET" \
	-e "PROVIDER_ID=${PROVIDER_ID:-provider-alpha}" \
	-e "RUN_ID=${RUN_ID:-}" \
	"$@" scripts/k6-concurrency-test.js