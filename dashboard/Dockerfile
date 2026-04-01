FROM python:3.12-slim

WORKDIR /opt/project

RUN apt-get update && apt-get install -y --no-install-recommends docker.io docker-cli && rm -rf /var/lib/apt/lists/*

COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt

CMD ["python", "/opt/project/dashboard/server.py"]
