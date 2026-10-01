#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

case "${1:-up}" in
	up)
		migration=/db/migrations/0001_financial_schema_up.sql
		;;
	down)
		migration=/db/migrations/0001_financial_schema_down.sql
		;;
	*)
		printf 'Uso: sh scripts/migrate.sh [up|down]\n' >&2
		exit 2
		;;
esac

docker compose exec -T postgres sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$1"' sh "$migration"