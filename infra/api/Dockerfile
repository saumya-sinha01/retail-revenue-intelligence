FROM python:3.11-slim

WORKDIR /app

# Copy requirements first (better layer caching)
COPY requirements.txt .

# Install dependencies using python -m pip (important)
RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY mock_logistics_api.py .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "mock_logistics_api:app", "--host", "0.0.0.0", "--port", "8000"]