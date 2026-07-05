FROM python:3.12-slim

WORKDIR /app

# Optional: trust an extra CA (corporate/CI TLS-intercepting proxies).
# Drop the cert into the build context as `extra-ca.crt` and it gets picked
# up; the glob matches zero-or-one files, pyproject.toml anchors the COPY.
COPY pyproject.toml extra-ca.crt* ./
RUN if [ -f extra-ca.crt ]; then \
        cp extra-ca.crt /usr/local/share/ca-certificates/extra-ca.crt && \
        update-ca-certificates && \
        pip config set global.cert /etc/ssl/certs/ca-certificates.crt; \
    fi

COPY shopstream_generator ./shopstream_generator

RUN pip install --no-cache-dir .

# Unbuffered so `docker compose logs` streams in real time
ENV PYTHONUNBUFFERED=1

CMD ["shopstream-generator"]
