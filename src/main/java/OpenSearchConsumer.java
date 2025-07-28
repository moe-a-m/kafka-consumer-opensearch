import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OpenSearchConsumer {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // --- Metrics ---
    private static final AtomicLong totalRecordsConsumed = new AtomicLong(0);
    private static final AtomicLong totalRecordsIndexed = new AtomicLong(0);
    private static final AtomicLong totalIndexErrors = new AtomicLong(0);
    private static final AtomicInteger currentBatchSize = new AtomicInteger(0);
    private static final AtomicLong totalProcessingTimeMs = new AtomicLong(0); // Time spent processing records
    private static final AtomicLong totalIndexingTimeMs = new AtomicLong(0);   // Time spent on OpenSearch bulk calls

    // --- Configuration ---
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(3000);

    // --- Shutdown Control ---
    private static volatile boolean shutdownRequested = false;
    private static final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    static int pollCycleCounter = 0;

    public static OpenSearchClient createOpenSearchClient() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        var env = System.getenv();
        // Consider making these configurable via environment variables or properties
        final HttpHost host = new HttpHost("http", "localhost", 9200);
        var user = env.getOrDefault("OPENSEARCH_USERNAME", "admin");
        var pass = env.getOrDefault("OPENSEARCH_PASSWORD", "admin");
        final var credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(user, pass.toCharArray()));
        final var sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build();

        final var transport = ApacheHttpClient5TransportBuilder.builder(host)
                .setMapper(new JacksonJsonpMapper(objectMapper)) // Pass your existing ObjectMapper
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    final var tlsStrategy = ClientTlsStrategyBuilder.create()
                            .setSslContext(sslContext)
                            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                            . buildAsync();
                    final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                })
                .build();
        return new OpenSearchClient(transport);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:19092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Unique group ID
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit for control
        // Add health check related configs if needed, e.g., timeouts
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000"); // Default is often 45s
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "15000"); // Default is often 1/3 of session timeout
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        try {
            if (json == null || json.trim().isEmpty()) {
                log.warn("Empty JSON string provided");
                return null;
            }
            JsonNode rootNode = objectMapper.readTree(json);
            JsonNode idNode = rootNode.path("meta").path("id");
            if (idNode.isMissingNode() || idNode.isNull()) {
                log.warn("ID field missing in JSON");
                return null;
            }
            String id = idNode.asText();
            if (id.trim().isEmpty()) {
                log.warn("Empty ID found in JSON");
                return null;
            }
            return id;
        } catch (Exception e) {
            log.error("Error parsing JSON for ID extraction", e);
            return null;
        }
    }

    private static void createIndexWithDynamicMapping(OpenSearchClient client, String indexName) throws IOException {
        try {
            // Check if index exists and delete it (for clean start, adjust as needed for production)
            if (client.indices().exists(r -> r.index(indexName)).value()) {
                log.info("Deleting existing index: {}", indexName);
                client.indices().delete(d -> d.index(indexName));
            }
            // Create index with dynamic mapping set to "true" (default behavior)
            client.indices().create(c -> c
                    .index(indexName)
                    .mappings(m -> m
                            .dynamic(DynamicMapping.True) // Explicitly set dynamic mapping
                    ));
            log.info("Created index {} with dynamic mapping enabled", indexName);
        } catch (Exception e) {
            log.error("Failed to create index with dynamic mapping", e);
            throw e; // Re-throw to prevent startup if index setup fails
        }
    }

    // --- Health Checks ---
    private static boolean isKafkaHealthy(KafkaConsumer<String, String> consumer) {
        try {
            // A simple check is to see if we can poll (non-blocking poll with timeout 0 might work,
            // but generally, if the consumer is created and not closed, it's considered healthy
            // unless there's a network partition or broker issue).
            // For a more active check, you might need to query metadata or use admin client.
            // Here we assume if no exception occurs during normal operation, it's healthy.
            // This is a basic placeholder.
            return consumer != null && !consumer.subscription().isEmpty(); // Check if subscribed
            // A more robust check might involve consumer.metrics() or trying to list topics.
        } catch (Exception e) {
            log.warn("Kafka health check failed", e);
            return false;
        }
    }

    private static boolean isOpenSearchHealthy(OpenSearchClient client) {
        try {
            var info = client.info();
            log.debug("OpenSearch health check successful: {} {}", info.version().distribution(), info.version().number());
            return true;
        } catch (Exception e) {
            log.warn("OpenSearch health check failed", e);
            return false;
        }
    }

    // --- Retry Logic ---
    private static BulkResponse performBulkIndexWithRetry(OpenSearchClient client, BulkRequest request) throws IOException, InterruptedException {
        IOException lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.debug("Attempting bulk index (attempt {}/{})", attempt, MAX_RETRIES);
                Instant start = Instant.now();
                BulkResponse response = client.bulk(request);
                long duration = Duration.between(start, Instant.now()).toMillis();
                totalIndexingTimeMs.addAndGet(duration);
                log.debug("Bulk index attempt {} successful in {} ms", attempt, duration);
                return response;
            } catch (IOException e) {
                lastException = e;
                log.warn("Bulk index attempt {}/{} failed: {}", attempt, MAX_RETRIES, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    log.info("Retrying in {} ms...", RETRY_BACKOFF_MS);
                    Thread.sleep(RETRY_BACKOFF_MS);
                } else {
                    log.error("All {} retry attempts failed for bulk index", MAX_RETRIES, e);
                }
            }
        }
        // If we get here, all retries failed
        throw new IOException("Failed to perform bulk index after " + MAX_RETRIES + " attempts", lastException);
    }

    // --- Metrics Logging ---
    private static void logMetrics() {
        long totalConsumed = totalRecordsConsumed.get();
        long totalIndexed = totalRecordsIndexed.get();
        long errors = totalIndexErrors.get();
        long processingTime = totalProcessingTimeMs.get();
        long indexingTime = totalIndexingTimeMs.get();
        int currentBatch = currentBatchSize.get();

        double avgProcessingTimePerRecord = (totalConsumed > 0) ? (double) processingTime / totalConsumed : 0.0;
        double avgIndexingTimePerRecord = (totalIndexed > 0) ? (double) indexingTime / totalIndexed : 0.0;

        log.info("=== Consumer Metrics ===");
        log.info("  Total Records Consumed: {}", totalConsumed);
        log.info("  Total Records Indexed: {}", totalIndexed);
        log.info("  Total Index Errors: {}", errors);
        log.info("  Current Batch Size: {}", currentBatch);
        log.info("  Avg Processing Time/Record (ms): %.2f".formatted(avgProcessingTimePerRecord));
        log.info("  Avg Indexing Time/Record (ms): %.2f".formatted(avgIndexingTimePerRecord));
        log.info("========================");
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = null;

        try {
            OpenSearchClient opensearchClient = createOpenSearchClient();
            consumer = createKafkaConsumer();

            // --- Health Checks ---
            log.info("Performing initial health checks...");
            if (isOpenSearchHealthy(opensearchClient)) {
                log.info("OpenSearch connection healthy.");
            } else {
                throw new RuntimeException("OpenSearch is not healthy at startup");
            }

            String index = "wikimedia";
            createIndexWithDynamicMapping(opensearchClient, index);

            // --- Graceful Shutdown Hook ---
            final KafkaConsumer<String, String> finalConsumer = consumer;
            final Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received.");
                shutdownRequested = true;
                if (shutdownInitiated.compareAndSet(false, true)) { // Ensure only one shutdown sequence
                    log.info("Initiating graceful shutdown...");
                    // Wake up the consumer if it's blocked in poll()
                    try {
                        finalConsumer.wakeup();
                    } catch (Exception e) {
                        log.warn("Exception during consumer wakeup", e);
                    }

                    try {
                        // Wait for the main loop to finish
                        mainThread.join(10000); // Wait up to 10 seconds
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Shutdown hook interrupted", e);
                    }
                    log.info("Shutdown hook completed.");
                }
            }));

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            log.info("Starting main consumption loop...");
            while (!shutdownRequested) {
                // --- Health Checks (Periodic) ---
                if (!isKafkaHealthy(consumer) || !isOpenSearchHealthy(opensearchClient)) {
                    log.error("Health check failed, initiating shutdown.");
                    shutdownRequested = true;
                    break;
                }

                log.info("Polling Kafka...");
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                int recordCount = records.count();
                totalRecordsConsumed.addAndGet(recordCount);
                log.info("Received: {} record(s)", recordCount);

                if (recordCount > 0) {
                    Instant processingStart = Instant.now();
                    ArrayList<BulkOperation> ops = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records) {
                        String id = extractId(record.value());
                        if (id == null || id.trim().isEmpty()) {
                            log.warn("Skipping record due to invalid ID");
                            continue;
                        }

                        try {
                            JsonNode documentNode = objectMapper.readTree(record.value());
                            IndexOperation<JsonNode> indexOp = IndexOperation.of(io ->
                                    io.index(index)
                                            .id(id)
                                            .document(documentNode)
                            );
                            BulkOperation bulkOp = new BulkOperation.Builder().index(indexOp).build();
                            ops.add(bulkOp);
                        } catch (Exception e) {
                            log.error("Error processing JSON document for ID {}", id, e);
                            totalIndexErrors.incrementAndGet(); // Count processing errors too
                            // Continue with other records
                        }
                    }

                    long processingDuration = Duration.between(processingStart, Instant.now()).toMillis();
                    totalProcessingTimeMs.addAndGet(processingDuration);
                    currentBatchSize.set(ops.size()); // Update current batch size metric

                    if (!ops.isEmpty()) {
                        log.info("Preparing to send bulk request with {} operations", ops.size());

                        try {
                            BulkRequest bulkRequest = new BulkRequest.Builder()
                                    .operations(ops)
                                    .refresh(Refresh.WaitFor) // Consider if this is needed for every bulk
                                    .build();

                            BulkResponse bulkResponse = performBulkIndexWithRetry(opensearchClient, bulkRequest);

                            log.info("Bulk response items: {}, errors: {}", bulkResponse.items().size(), bulkResponse.errors());

                            if (bulkResponse.errors()) {
                                long errorCount = 0;
                                for (var item : bulkResponse.items()) {
                                    if (item.error() != null) {
                                        errorCount++;
                                        log.error("Bulk item error - Index: '{}', ID: '{}', Type: '{}', Reason: '{}'",
                                                item.index(), item.id(), item.error().type(), item.error().reason());
                                    }
                                }
                                totalIndexErrors.addAndGet(errorCount);
                            }
                            totalRecordsIndexed.addAndGet(bulkResponse.items().size() - (bulkResponse.errors() ? 1 : 0)); // Approximate, errors complicate exact count

                        } catch (IOException | InterruptedException e) {
                            log.error("Failed to index batch after retries or interrupted", e);
                            totalIndexErrors.addAndGet(ops.size()); // Assume all failed if retries exhausted or interrupted
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt(); // Preserve interrupt status
                                break; // Exit loop on interrupt
                            }
                        }
                    } else {
                        log.info("No valid operations to send in bulk request");
                    }
                } else {
                    log.debug("No records to process in this poll cycle");
                }

                // --- Commit offsets manually ---
                try {
                    consumer.commitSync();
                    log.debug("Committed consumer offsets");
                } catch (Exception e) {
                    log.error("Error committing offsets", e);
                    // Depending on requirements, you might want to initiate shutdown or retry
                }

                // --- Periodic Metrics Logging ---
                // Simple way: log every 10 poll cycles or based on a counter

                if (++pollCycleCounter % 10 == 0) {
                    logMetrics();
                }
            }

        } catch (WakeupException e) {
            // This is expected during shutdown initiated by wakeup()
            log.info("Consumer woken up, likely due to shutdown request.");
        } catch (Exception e) {
            log.error("Unexpected error in main loop", e);
        } finally {
            log.info("Entering final shutdown sequence...");
            shutdownRequested = true; // Ensure flag is set

            // --- Graceful Shutdown: Flush pending operations ---
            // In this simple case, we process everything in the poll loop.
            // More complex apps might have a queue/buffer to flush here.
            // If you had a buffer, you'd drain and index it here before closing.

            // --- Close resources ---
            if (consumer != null) {
                try {
                    log.info("Closing Kafka consumer...");
                    consumer.close(Duration.ofSeconds(10)); // Allow time for final commit
                    log.info("Kafka consumer closed.");
                } catch (Exception e) {
                    log.error("Error closing Kafka consumer", e);
                }
            }

            // Note: OpenSearchClient doesn't have a standard close method in older versions.
            // Newer versions or transports might. Check your specific client version docs.
            // If needed, you might close the underlying transport if accessible.
            // For ApacheHttpClient5Transport, there isn't a standard close() on the client itself.
            // The connection manager handles connection lifecycle.

            logMetrics(); // Log final metrics
            log.info("Consumer gracefully shutdown completed.");
        }
    }
}