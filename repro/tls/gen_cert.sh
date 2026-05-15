#!/bin/sh
# Regenerate the self-signed cert + key used by stunnel to TLS-front the
# emulator on port 5671 (required by uamqp_transport=True, which only
# speaks TLS). Idempotent; safe to re-run.
#
# cert.pem and key.pem are intentionally NOT committed (emulator-only
# fixtures).
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

if [ -f cert.pem ] && [ -f key.pem ]; then
    echo "cert.pem and key.pem already exist; remove them first to regenerate."
    exit 0
fi

openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
    -sha256 -days 30 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
chmod 600 key.pem
echo "wrote cert.pem and key.pem in $DIR"
