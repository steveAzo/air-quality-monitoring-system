FROM python:3.11-slim

# Set work dir
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y build-essential libpq-dev

# Copy requirements
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port
EXPOSE 8080

# Run FastAPI with uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
