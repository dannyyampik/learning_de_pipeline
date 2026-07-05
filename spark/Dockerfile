FROM apache/spark:3.5.3

USER root

# Optional: trust an extra CA (corporate/CI TLS-intercepting proxies).
# Same pattern as the other images — a no-op if extra-ca.crt is absent.
COPY spark-defaults.conf extra-ca.crt* /tmp/build/
RUN if [ -f /tmp/build/extra-ca.crt ]; then \
        cp /tmp/build/extra-ca.crt /usr/local/share/ca-certificates/extra-ca.crt && \
        update-ca-certificates && \
        pip config set global.cert /etc/ssl/certs/ca-certificates.crt; \
    fi

# Bake the connector jars into the image instead of using --packages at
# runtime: reproducible, offline-friendly, and you see exactly what a
# "Spark + Kafka + Iceberg" runtime is made of.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -e; cd /opt/spark/jars && \
    curl -fsSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $MAVEN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSLO $MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSLO $MAVEN/org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar && \
    curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar

# Python deps used by the jobs (Schema Registry REST + ClickHouse sink).
# clickhouse-connect pinned <0.8: this Spark image ships Python 3.8.
RUN pip install --no-cache-dir requests "clickhouse-connect<0.8"

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
