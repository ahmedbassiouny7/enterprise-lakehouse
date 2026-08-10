FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_and_load.py seed_incremental.py .

# Runs once and exits — this is a job container, not a long-running service.
CMD ["python", "generate_and_load.py"]