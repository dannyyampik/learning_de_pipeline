FROM apache/airflow:3.0.2

# Optional: trust an extra CA (corporate/CI TLS-intercepting proxies).
# Same pattern as generator/Dockerfile — a no-op if extra-ca.crt is absent.
USER root
COPY requirements.txt extra-ca.crt* /tmp/build/
RUN if [ -f /tmp/build/extra-ca.crt ]; then \
        cp /tmp/build/extra-ca.crt /usr/local/share/ca-certificates/extra-ca.crt && \
        update-ca-certificates; \
    fi

USER airflow
RUN if [ -f /tmp/build/extra-ca.crt ]; then \
        pip config set global.cert /etc/ssl/certs/ca-certificates.crt; \
    fi && \
    pip install --no-cache-dir -r /tmp/build/requirements.txt
