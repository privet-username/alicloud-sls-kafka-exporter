FROM python:3.11-slim

# Force the stdout and stderr streams to be unbuffered
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and certificates
# Note: Ensure alicloud_logservice.py and .pem files are in the 'app' directory locally
COPY app/ .

ENTRYPOINT [ "python", "alicloud_logservice.py" ]