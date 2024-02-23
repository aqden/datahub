# FROM python:3.8-slim as base
# RUN apt-get update && apt-get install -y jq
# RUN apt update && apt install -y build-essential python3-dev

# # FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim as base
# FROM openjdk:8 as prod-build
# COPY . /datahub-src
# RUN cd /datahub-src && ./gradlew :metadata-ingestion:codegen -d

# FROM base as prod-codegen
# COPY --from=prod-build /datahub-src /datahub-src
# RUN cd /datahub-src/metadata-ingestion && \
#     pip install -e ".[base]" 

# FROM base as final
# ENV PROMETHEUS_MULTIPROC_DIR=/gunicorn/
# COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
# RUN pip install /datahub-ingestion/.
# COPY ./ingest-api/ /ingest-api/
# WORKDIR /ingest-api/ingest_api
# RUN pip install -r ../requirements.txt && pip install ../ && mkdir /gunicorn/
# CMD ["gunicorn", "-c", "config.py", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8001"]

ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head

FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0
COPY ./metadata-ingestion /datahub-ingestion
WORKDIR /datahub-ingestion
RUN pip install --no-cache --user ".[base]"

FROM base as final
ENV PROMETHEUS_MULTIPROC_DIR=/gunicorn/
COPY ./ingest-api/ /ingest-api/
WORKDIR /ingest-api/ingest_api
RUN pip install -r ../requirements.txt && pip install ../ && mkdir /gunicorn/
ENTRYPOINT []
CMD ["gunicorn", "-c", "config.py", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8001"]
