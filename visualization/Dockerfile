FROM ghcr.io/astral-sh/uv:0.8.14-python3.12-alpine@sha256:c8dadf660e267203b683cd18e20b919a1d2ce4031cc3e2b4e5f381e3ac823fd8
LABEL authors="rivaults"
COPY --from=ghcr.io/tarampampam/curl:8.6.0 /bin/curl /bin/curl

ADD . /app
WORKDIR /app
RUN uv sync --locked
CMD ["uv", "run", "app"]