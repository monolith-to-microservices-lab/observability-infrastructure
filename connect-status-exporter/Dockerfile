FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY exporter.py .

# Non-root runtime user (numeric UID so Kubernetes/ECS can verify it).
RUN useradd --create-home --uid 1000 appuser && chown -R 1000:1000 /app
USER 1000

EXPOSE 9877
CMD ["python", "exporter.py"]
