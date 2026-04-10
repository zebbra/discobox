# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# Proxy support — passed through automatically from the build environment
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

# Place ca-bundle.pem in repo root before building (gitignored):
#   cp /etc/ssl/ca-bundle.pem .
COPY ca-bundle.pem /etc/ssl/ca-bundle.pem

COPY requirements.txt .
RUN pip install --no-cache-dir --cert /etc/ssl/ca-bundle.pem -r requirements.txt

COPY discobox.py server.py cli.py ./

# Server mode by default; use cli.py for one-shot syncs
ENTRYPOINT ["python", "server.py"]
