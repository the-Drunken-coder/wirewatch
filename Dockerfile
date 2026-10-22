FROM node:24-bookworm-slim AS ui-build
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

FROM node:24-bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global @openai/codex@0.163.0 \
    && npm cache clean --force
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 CODEX_HOME=/home/node/.codex
WORKDIR /app
COPY diagnostics.py engine.py model.py setup_review.py server.py ./
COPY --from=ui-build /app/static/ ./static/
RUN mkdir -p /data /home/node/.codex \
    && printf 'cli_auth_credentials_store = "file"\n' > /home/node/.codex/config.toml \
    && chown -R node:node /data /home/node/.codex
USER node
EXPOSE 8765
CMD ["python3", "server.py", "--host", "0.0.0.0", "--db", "/data/wirewatch.sqlite3"]
