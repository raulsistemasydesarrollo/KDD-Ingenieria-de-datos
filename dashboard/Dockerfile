FROM python:3.12-slim

WORKDIR /opt/project

COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt

CMD ["python", "/opt/project/dashboard/server.py"]
