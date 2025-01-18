#!/usr/bin/env bash
set -e

if ! [ -e /etc/ozmadb/config.json ]; then
  if [ -z "$DB_HOST" ]; then
    echo "DB_HOST must be set" >&2
    exit 1
  fi
  if [ -z "$DB_PORT" ]; then
    DB_PORT=5432
  fi
  if [ -z "$DB_USER" ]; then
    echo "DB_USER must be set" >&2
    exit 1
  fi
  if [ -z "$DB_PASSWORD" ]; then
    echo "DB_PASSWORD must be set" >&2
    exit 1
  fi
  if [ -z "$DB_NAME" ]; then
    echo "DB_NAME must be set" >&2
    exit 1
  fi

  if [ -z "$EXTERNAL_ORIGIN" ]; then
    if [ -n "$EXTERNAL_HOSTPORT" ]; then
      EXTERNAL_ORIGIN="${EXTERNAL_PROTOCOL:-http}://${EXTERNAL_HOSTPORT}"
    fi
  fi

  if [ -z "$AUTH_AUTHORITY" ]; then
    if [ -n "$EXTERNAL_ORIGIN" ]; then
      AUTH_AUTHORITY="${EXTERNAL_ORIGIN}/auth/realms/ozma"
    else
      echo "AUTH_AUTHORITY must be set" >&2
      exit 1
    fi
  fi

  if [ -z "$PRELOAD" ] && [ -e /etc/ozmadb/preload.json ]; then
    PRELOAD=/etc/ozmadb/preload.json
  fi

  if [ -z "$REDIS" ]; then
    if [ -n "$REDIS_HOST" ]; then
      REDIS="${REDIS_HOST}:${REDIS_PORT:-6379}"
    fi
  fi

  mkdir -p /etc/ozmadb
  jq -n \
    --arg dbHost "$DB_HOST" \
    --argjson dbPort "$DB_PORT" \
    --arg dbUser "$DB_USER" \
    --arg dbPassword "$DB_PASSWORD" \
    --arg dbName "$DB_NAME" \
    --arg authAuthority "$AUTH_AUTHORITY" \
    --arg authMetadataAddress "$AUTH_METADATA_ADDRESS" \
    --argjson authRequireHttpsMetadata "${AUTH_REQUIRE_HTTPS_METADATA:-true}" \
    --arg preload "$PRELOAD" \
    --arg redis "$REDIS" \
    '{
      "kestrel": {
        "endpoints": {
          "http": {
            "url": "http://0.0.0.0:5000"
          }
        }
      },
      "serilog": {
        "minimumLevel": {
          "default": "Verbose"
        }
      },
      "ozmaDB": ({
        "authAuthority": $authAuthority,
        "authRequireHttpsMetadata": $authRequireHttpsMetadata,
        "allowAutoMark": true,
        "instancesSource": "static",
        "instance": {
          "host": $dbHost,
          "port": $dbPort,
          "username": $dbUser,
          "password": $dbPassword,
          "database": $dbName
        }
      } + (if $preload == "" then {} else {"preload": $preload} end)
        + (if $authMetadataAddress == "" then {} else {"authMetadataAddress": $authMetadataAddress} end)
        + (if $redis == "" then {} else {"redis": $redis} end))
    }' > /etc/ozmadb/config.json
fi

exec /opt/ozmadb/OzmaDB /etc/ozmadb/config.json
