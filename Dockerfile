# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

# Pass corporate CA bundle at build time if needed:
#   docker compose build --secret id=ca_bundle,src=./ca-bundle.pem
RUN --mount=type=secret,id=ca_bundle,target=/tmp/ca-bundle.pem \
    pip install --no-cache-dir \
        $([ -f /tmp/ca-bundle.pem ] && echo "--cert /tmp/ca-bundle.pem") \
        -r requirements.txt

COPY discobox.py server.py cli.py ./

# Server mode by default; use cli.py for one-shot syncs
ENTRYPOINT ["python", "server.py"]
