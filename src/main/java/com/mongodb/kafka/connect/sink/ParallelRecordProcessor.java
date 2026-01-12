/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTask.LOGGER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Processes SinkRecords in parallel while preserving ordering for the final output.
 *
 * <p>Thread Safety Guarantees:
 *
 * <ul>
 *   <li>Each record is processed independently with no shared mutable state
 *   <li>Original ordering is restored via the record's original index after parallel processing
 *   <li>Errors in individual records are captured in MongoProcessedSinkRecordData, not thrown
 *       immediately
 * </ul>
 *
 * <p>This class follows the same pattern as {@link
 * com.mongodb.kafka.connect.source.MongoCopyDataManager} which uses a thread pool for parallel work
 * in the source connector.
 */
final class ParallelRecordProcessor implements AutoCloseable {

  private final ExecutorService executor;
  private final int parallelism;
  private final MongoSinkConfig sinkConfig;

  /**
   * Creates a new ParallelRecordProcessor.
   *
   * @param parallelism Number of threads to use. If 1, processing is sequential (no thread pool
   *     created).
   * @param queueSize Size of the work queue for the thread pool.
   * @param sinkConfig The sink configuration.
   */
  ParallelRecordProcessor(
      final int parallelism, final int queueSize, final MongoSinkConfig sinkConfig) {
    this.parallelism = parallelism;
    this.sinkConfig = sinkConfig;

    if (parallelism > 1) {
      LOGGER.info(
          "Initializing parallel record processor with {} threads and queue size {}",
          parallelism,
          queueSize);
      this.executor =
          new ThreadPoolExecutor(
              parallelism,
              parallelism,
              60L,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(queueSize),
              new ThreadPoolExecutor
                  .CallerRunsPolicy() // Backpressure: caller thread processes when queue full
              );
    } else {
      LOGGER.debug("Parallel record processing disabled (parallelism=1)");
      this.executor = null;
    }
  }

  /**
   * Processes SinkRecords and returns processed data.
   *
   * <p>If parallelism is enabled and the batch is large enough, processing occurs in parallel.
   * Order is always preserved in the returned list.
   *
   * @param records The records to process
   * @return List of processed records in the same order as input
   */
  List<MongoProcessedSinkRecordData> process(final Collection<SinkRecord> records) {
    if (parallelism <= 1 || records.size() < parallelism) {
      // Fall back to sequential for small batches or when disabled
      return processSequentially(records);
    }
    return processInParallel(records);
  }

  private List<MongoProcessedSinkRecordData> processInParallel(
      final Collection<SinkRecord> records) {
    LOGGER.debug("Processing {} records in parallel with {} threads", records.size(), parallelism);

    // Create indexed records to preserve ordering
    List<IndexedRecord> indexedRecords = new ArrayList<>(records.size());
    int index = 0;
    for (SinkRecord record : records) {
      indexedRecords.add(new IndexedRecord(index++, record));
    }

    // Process in parallel using CompletableFuture
    List<CompletableFuture<IndexedProcessedRecord>> futures =
        indexedRecords.stream()
            .map(
                ir ->
                    CompletableFuture.supplyAsync(
                        () ->
                            new IndexedProcessedRecord(
                                ir.index, new MongoProcessedSinkRecordData(ir.record, sinkConfig)),
                        executor))
            .collect(Collectors.toList());

    // Wait for all to complete and restore original order
    return futures.stream()
        .map(CompletableFuture::join)
        .sorted(Comparator.comparingInt(ipr -> ipr.index))
        .map(ipr -> ipr.processed)
        .collect(Collectors.toList());
  }

  private List<MongoProcessedSinkRecordData> processSequentially(
      final Collection<SinkRecord> records) {
    return records.stream()
        .map(r -> new MongoProcessedSinkRecordData(r, sinkConfig))
        .collect(Collectors.toList());
  }

  @Override
  public void close() {
    if (executor != null) {
      LOGGER.debug("Shutting down parallel record processor");
      executor.shutdown();
      try {
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          LOGGER.warn("Parallel processor did not terminate gracefully, forcing shutdown");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for parallel processor shutdown");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  // Helper class to track original record index
  private static final class IndexedRecord {
    final int index;
    final SinkRecord record;

    IndexedRecord(final int index, final SinkRecord record) {
      this.index = index;
      this.record = record;
    }
  }

  // Helper class to track processed record with original index
  private static final class IndexedProcessedRecord {
    final int index;
    final MongoProcessedSinkRecordData processed;

    IndexedProcessedRecord(final int index, final MongoProcessedSinkRecordData processed) {
      this.index = index;
      this.processed = processed;
    }
  }
}
