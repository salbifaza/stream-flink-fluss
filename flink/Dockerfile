# One image for the jobmanager, taskmanager and SQL client, so every process
# sees identical jars:
#   - Flink CDC 3.6 (CLI + Postgres source + Fluss sink pipeline connectors)
#   - the Fluss Flink connector (Fluss catalog, tables and $changelog reads)
#   - Flink's JDBC connector + the ClickHouse JDBC driver
#   - a ClickHouse dialect for the JDBC connector, compiled in stage 1
#     (see flink/clickhouse-dialect/pom.xml for why it exists)

# ── stage 1: build the ClickHouse JDBC dialect ──────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS dialect
WORKDIR /build
COPY clickhouse-dialect/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY clickhouse-dialect/src ./src
RUN mvn -q -B package -DskipTests

# ── stage 2: Flink runtime ──────────────────────────────────────────────────
FROM flink:1.20-scala_2.12-java11

ARG FLUSS_VERSION=0.9.1-incubating
ARG FLINK_CDC_VERSION=3.6.0
ARG FLINK_JDBC_VERSION=3.3.0-1.20
ARG CLICKHOUSE_JDBC_VERSION=0.9.9

USER root

# Fluss <-> Flink connector: the Fluss catalog, PK tables and $changelog reads.
ADD https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/${FLUSS_VERSION}/fluss-flink-1.20-${FLUSS_VERSION}.jar \
    /opt/flink/lib/

# JDBC sink + ClickHouse driver (-all: shaded, no transitive jars needed) + dialect.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/${FLINK_JDBC_VERSION}/flink-connector-jdbc-core-${FLINK_JDBC_VERSION}.jar \
    /opt/flink/lib/
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar \
    /opt/flink/lib/
COPY --from=dialect /build/target/flink-jdbc-clickhouse-dialect-1.0.0.jar /opt/flink/lib/

# Flink CDC CLI (flink-cdc.sh) and its pipeline connectors.
ADD https://dist.apache.org/repos/dist/release/flink/flink-cdc-${FLINK_CDC_VERSION}/flink-cdc-${FLINK_CDC_VERSION}-1.20-bin.tar.gz \
    /tmp/flink-cdc.tar.gz
RUN mkdir -p /opt/flink-cdc && \
    tar -xzf /tmp/flink-cdc.tar.gz -C /opt/flink-cdc --strip-components=1 && \
    rm /tmp/flink-cdc.tar.gz
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/${FLINK_CDC_VERSION}-1.20/flink-cdc-pipeline-connector-postgres-${FLINK_CDC_VERSION}-1.20.jar \
    /opt/flink-cdc/lib/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/${FLINK_CDC_VERSION}-1.20/flink-cdc-pipeline-connector-fluss-${FLINK_CDC_VERSION}-1.20.jar \
    /opt/flink-cdc/lib/
ENV FLINK_CDC_HOME=/opt/flink-cdc

# Pipeline definition, SQL jobs and submit scripts.
COPY postgres-to-fluss.yaml run-cdc-pipeline.sh run-sql-jobs.sh /opt/pipelines/
COPY sql/ /opt/pipelines/sql/

# Checkpoint directory, shared by jobmanager + taskmanager through a named
# volume. Created here so the volume inherits flink-user ownership.
RUN mkdir -p /opt/flink/checkpoints && \
    chmod +x /opt/pipelines/*.sh && \
    chown -R flink:flink /opt/flink/lib /opt/flink-cdc /opt/pipelines /opt/flink/checkpoints

USER flink
