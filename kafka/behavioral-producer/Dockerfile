FROM python:3.10-slim

WORKDIR /project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY schemas/ ./schemas/

ENV PYTHONPATH=/project

CMD ["python", "app/producer.py"]
