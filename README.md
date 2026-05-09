# AliCloud SLS to Kafka Log Exporter

A Python service designed to stream logs from **Alibaba Cloud Log Service (SLS)** directly to **Apache Kafka**. Optimized for exporting ActionTrail logs for security monitoring and SIEM integration.

## Key Features
* **Production-Ready:** Optimized Kafka producer with batch flushing for high throughput.
* **Fully Configurable:** No hardcoded values; all parameters are controlled via environment variables.
* **Secure:** Supports SSL/TLS authentication for Kafka connections.
* **Observability:** Integrated logging for real-time monitoring of the export process.

## Prerequisites
* **Python 3.11+** or **Docker**.
* An AliCloud Log Service Project and Logstore.
* Access to a Kafka Cluster with SSL enabled.

## Configuration

The service is configured using Environment Variables.

### Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `ALICLOUD_ENDPOINT` | **Yes** | - | AliCloud SLS region endpoint. |
| `ALICLOUD_KEY_ID` | **Yes** | - | AliCloud Access Key ID. |
| `ALICLOUD_KEY` | **Yes** | - | AliCloud Access Key Secret. |
| `ALICLOUD_PROJECT` | **Yes** | - | SLS Project name. |
| `ALICLOUD_LOGSTORE` | **Yes** | - | SLS Logstore name. |
| `KAFKA_SERVERS` | **Yes** | - | Comma-separated Kafka brokers (host:port). |
| `KAFKA_TOPIC` | **Yes** | - | Target Kafka topic. |
| `CONSUMER_GROUP` | No | `alicloud-actiontrail...` | SLS Consumer group name. |
| `CURSOR_START_TIME` | No | `2018-12-26 0:0:0` | Initial cursor position. |
| `KAFKA_SSL_CA` | No | `kafka-ca.pem` | Filename of the CA certificate. |
| `KAFKA_SSL_CERT` | No | `producer-cert.pem` | Filename of the client certificate. |
| `KAFKA_SSL_KEY` | No | `producer-key.pem` | Filename of the client key. |
| `LOG_HOST` | No | `actiontrail-logs...` | Value for `log_host` field in output. |

## Deployment

### Local Directory Structure
```
.
├── Dockerfile
├── requirements.txt
└── app/
    ├── alicloud_logservice.py
    ├── kafka-ca.pem
    ├── producer-cert.pem
    └── producer-key.pem
```

## Building and Running with Docker

1. Build the image:
```
docker build -t alicloud-sls-kafka-exporter .
```

2. Run the container:
```
docker run -d \
  --name exporter \
  -e ALICLOUD_ENDPOINT="cn-shanghai.log.aliyuncs.com" \
  -e ALICLOUD_KEY_ID="LTAI..." \
  -e ALICLOUD_KEY="secret..." \
  -e ALICLOUD_PROJECT="my-project" \
  -e ALICLOUD_LOGSTORE="my-logstore" \
  -e KAFKA_SERVERS="my-kafka:9093" \
  -e KAFKA_TOPIC="security-logs" \
  alicloud-sls-kafka-exporter
```

## License

MIT
