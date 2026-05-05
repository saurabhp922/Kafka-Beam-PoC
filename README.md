# Kafka-Beam-PoC

## What this does
- Reads messages from Kafka (input-topic)
- Processes using Apache Beam
- Writes to:
  - Kafka (output-topic)
  - MinIO (S3-compatible storage)

## How to run

1. Start infra:
docker-compose up -d

2. Run pipeline:
Run MainPipeline.java from IntelliJ

3. Test Kafka:
kafka-console-producer --topic input-topic --bootstrap-server localhost:9092
