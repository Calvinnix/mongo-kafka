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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Uploads batches to MongoDB in parallel while respecting ordering constraints.
 *
 * <p>Thread Safety Guarantees:
 *
 * <ul>
 *   <li>Each batch is uploaded independently with no shared mutable state between batches
 *   <li>When bulk.write.ordered is true, parallelism is automatically set to 1 to preserve order
 *   <li>Errors in individual batches are handled by the provided batch processor
 * </ul>
 */
final class ParallelBatchUploader implements AutoCloseable {

  private final ExecutorService executor;
  private final int parallelism;
  private final boolean bulkWriteOrdered;

  /**
   * Creates a new ParallelBatchUploader.
   *
   * @param parallelism Number of threads to use. If 1, uploads are sequential (no thread pool
   *     created). Automatically set to 1 if bulkWriteOrdered is true.
   * @param queueSize Size of the work queue for the thread pool.
   * @param bulkWriteOrdered Whether ordered bulk writes are enabled.
   */
  ParallelBatchUploader(
      final int parallelism, final int queueSize, final boolean bulkWriteOrdered) {
    this.bulkWriteOrdered = bulkWriteOrdered;
    // Force parallelism to 1 if ordered writes are enabled to preserve write order
    this.parallelism = bulkWriteOrdered ? 1 : parallelism;

    if (this.parallelism > 1) {
      LOGGER.info(
          "Initializing parallel batch uploader with {} threads and queue size {}",
          this.parallelism,
          queueSize);
      this.executor =
          new ThreadPoolExecutor(
              this.parallelism,
              this.parallelism,
              60L,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(queueSize),
              new ThreadPoolExecutor
                  .CallerRunsPolicy() // Backpressure: caller thread uploads when queue full
              );
    } else {
      if (bulkWriteOrdered && parallelism > 1) {
        LOGGER.info(
            "Parallel batch upload disabled because bulk.write.ordered=true. "
                + "Set bulk.write.ordered=false to enable parallel batch uploads.");
      } else {
        LOGGER.debug("Parallel batch upload disabled (parallelism=1)");
      }
      this.executor = null;
    }
  }

  /**
   * Uploads batches using the provided batch processor.
   *
   * <p>If parallelism is enabled, batches are uploaded in parallel. Otherwise, batches are uploaded
   * sequentially.
   *
   * @param batches The batches to upload
   * @param batchProcessor The function to process each batch (performs the actual MongoDB write)
   */
  void uploadBatches(
      final List<List<MongoProcessedSinkRecordData>> batches,
      final Consumer<List<MongoProcessedSinkRecordData>> batchProcessor) {
    if (parallelism <= 1 || batches.size() <= 1) {
      // Sequential upload for small batch counts or when disabled
      uploadSequentially(batches, batchProcessor);
    } else {
      uploadInParallel(batches, batchProcessor);
    }
  }

  private void uploadInParallel(
      final List<List<MongoProcessedSinkRecordData>> batches,
      final Consumer<List<MongoProcessedSinkRecordData>> batchProcessor) {
    LOGGER.debug("Uploading {} batches in parallel with {} threads", batches.size(), parallelism);

    // Submit all batches for parallel processing
    List<CompletableFuture<Void>> futures =
        batches.stream()
            .map(batch -> CompletableFuture.runAsync(() -> batchProcessor.accept(batch), executor))
            .collect(Collectors.toList());

    // Wait for all batches to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  private void uploadSequentially(
      final List<List<MongoProcessedSinkRecordData>> batches,
      final Consumer<List<MongoProcessedSinkRecordData>> batchProcessor) {
    for (List<MongoProcessedSinkRecordData> batch : batches) {
      batchProcessor.accept(batch);
    }
  }

  /**
   * Returns the effective parallelism being used.
   *
   * @return the parallelism value (1 if ordered writes are enabled)
   */
  int getEffectiveParallelism() {
    return parallelism;
  }

  @Override
  public void close() {
    if (executor != null) {
      LOGGER.debug("Shutting down parallel batch uploader");
      executor.shutdown();
      try {
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          LOGGER.warn("Parallel batch uploader did not terminate gracefully, forcing shutdown");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for parallel batch uploader shutdown");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
