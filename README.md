# Kafka to OpenSearch Consumer

A simple, robust Java application that consumes real-time JSON events from a Kafka topic and indexes them into OpenSearch with batching, retries, and metrics.

## Features

- Consumes messages from a Kafka topic (`wikimedia.recentchange`)
- Securely connects to OpenSearch using HTTPS and basic authentication
- Extracts document ID from `meta.id` field for idempotent indexing
- Uses batched bulk indexing (configurable size) for performance
- Implements retry logic with exponential backoff for fault tolerance
- Supports graceful shutdown via interrupt or kill signal
- Manual offset management to prevent data loss
- Dynamic index mapping for flexible schema ingestion
- Comprehensive logging and metrics (records consumed, indexed, errors, latency)

## Requirements

- Java 11+
- Apache Kafka running (default: `127.0.0.1:19092`)
- OpenSearch running (default: `http://localhost:9200`)
- OpenSearch username/password (via `OPENSEARCH_USERNAME` and `OPENSEARCH_PASSWORD`, defaults to `admin`/`admin`)

## Usage

1. Build the project (assuming Maven/Gradle or compile manually with dependencies)
2. Run the main class:
   ```bash
   java OpenSearchConsumer
   ```
3. Ensure the `wikimedia.recentchange` Kafka topic exists and is producing data

## Configuration

You can customize behavior by modifying constants in the class:
- `BATCH_SIZE`: Number of records to index per bulk request
- `MAX_RETRIES`: Retry attempts for failed bulk operations
- `RETRY_BACKOFF_MS`: Delay between retries
- Kafka and OpenSearch connection settings

## Metrics

The consumer logs periodic and final metrics including:
- Total records consumed and indexed
- Indexing errors
- Average processing and indexing time per record

## Shutdown

Send an interrupt signal (`Ctrl+C`) or kill command to trigger graceful shutdown. The consumer will:
- Stop polling
- Process any remaining records
- Commit offsets
- Close connections cleanly

---

*Note: This is a standalone demo. For production use, consider packaging with build tools and externalizing configuration.*