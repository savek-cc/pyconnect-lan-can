FROM python:3.12-slim

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir . \
 && mkdir -p /data \
 && chown -R app:app /data

COPY docker/entrypoint.sh /usr/local/bin/entrypoint
RUN chmod +x /usr/local/bin/entrypoint

USER app
VOLUME ["/data"]

ENTRYPOINT ["entrypoint"]
