FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY discobox.py server.py cli.py ./

# Server mode by default; use cli.py for one-shot syncs
ENTRYPOINT ["python", "server.py"]
