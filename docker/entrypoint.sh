#!/bin/sh -e

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
  if [ -z "$AUTH_AUTHORITY" ]; then
    echo "AUTH_AUTHORITY must be set" >&2
    exit 1
  fi

  mkdir -p /etc/ozmadb
  jq -n \
    --arg dbHost "$DB_HOST" \
    --arg dbPort "$DB_PORT" \
    --arg dbUser "$DB_USER" \
    --arg dbPassword "$DB_PASSWORD" \
    --arg dbName "$DB_NAME" \
    --arg authAuthority "$AUTH_AUTHORITY" \
    '{
      url: "http://0.0.0.0:5000",
      connectionString: "Host=\($dbHost); Port=\($dbPort); Username=\($dbUser); Password=\($dbPassword); Database=\($dbName)",
      authAuthority: $authAuthority,
    }' > /etc/ozmadb/config.json
fi

exec /opt/ozmadb/OzmaDB /etc/ozmadb/config.json
