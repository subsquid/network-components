FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY traffic_generator.py .
COPY config.json .
COPY query_templates ./query_templates

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "./traffic_generator.py"]
