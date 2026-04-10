# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

# Mount ca-bundle.pem from build context if present (place it in repo root).
# If absent the mount is skipped and pip falls back to the system bundle.
RUN --mount=type=bind,source=ca-bundle.pem,target=/tmp/ca-bundle.pem,required=false \
    pip install --no-cache-dir \
        $([ -f /tmp/ca-bundle.pem ] && echo "--cert /tmp/ca-bundle.pem") \
        -r requirements.txt

COPY discobox.py server.py cli.py ./

# Server mode by default; use cli.py for one-shot syncs
ENTRYPOINT ["python", "server.py"]
