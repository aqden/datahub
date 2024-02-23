#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

DATAHUB_SEARCH_IMAGE="${DATAHUB_SEARCH_IMAGE:=opensearchproject/opensearch}"
DATAHUB_SEARCH_TAG="${DATAHUB_SEARCH_TAG:=2.9.0}"
XPACK_SECURITY_ENABLED="${XPACK_SECURITY_ENABLED:=plugins.security.disabled=true}"
ELASTICSEARCH_USE_SSL="${ELASTICSEARCH_USE_SSL:=false}"
USE_AWS_ELASTICSEARCH="${USE_AWS_ELASTICSEARCH:=true}"

echo "Point to our repo for the smoke test"
DATAHUB_FRONTEND_IMAGE="${DATAHUB_FRONTEND_IMAGE:-aqden/datahub-frontend-react}"
DATAHUB_GMS_IMAGE="${DATAHUB_GMS_IMAGE:-aqden/datahub-gms}"
DATAHUB_UPGRADE_IMAGE="${DATAHUB_UPGRADE_IMAGE:-aqden/datahub-upgrade}"
DATAHUB_ELASTIC_SETUP_IMAGE="${DATAHUB_ELASTIC_SETUP_IMAGE:-aqden/datahub-elasticsearch-setup}"
DATAHUB_KAFKA_SETUP_IMAGE="${DATAHUB_KAFKA_SETUP_IMAGE:-aqden/datahub-kafka-setup}"
DATAHUB_MYSQL_SETUP_IMAGE="${DATAHUB_MYSQL_SETUP_IMAGE:-aqden/datahub-mysql-setup}"

echo "DATAHUB_VERSION = $DATAHUB_VERSION"
DATAHUB_TELEMETRY_ENABLED=false  \
DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
DATAHUB_SEARCH_IMAGE="$DATAHUB_SEARCH_IMAGE" DATAHUB_SEARCH_TAG="$DATAHUB_SEARCH_TAG" \
XPACK_SECURITY_ENABLED="$XPACK_SECURITY_ENABLED" ELASTICSEARCH_USE_SSL="$ELASTICSEARCH_USE_SSL" \
USE_AWS_ELASTICSEARCH="$USE_AWS_ELASTICSEARCH" \
datahub docker quickstart --version ${DATAHUB_VERSION} --standalone_consumers --dump-logs-on-failure --kafka-setup
