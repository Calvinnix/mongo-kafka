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

import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createSinkConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ParallelBatchUploaderTest {

  @Test
  @DisplayName("test sequential upload when parallelism is 1")
  void testSequentialUpload() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(5, 10);
    List<Integer> processedBatchSizes = Collections.synchronizedList(new ArrayList<>());

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(1, 100, false)) {
      uploader.uploadBatches(batches, batch -> processedBatchSizes.add(batch.size()));

      assertEquals(5, processedBatchSizes.size());
      // All batches should have 10 records each
      processedBatchSizes.forEach(size -> assertEquals(10, size));
    }
  }

  @Test
  @DisplayName("test parallel upload when parallelism > 1 and unordered writes")
  void testParallelUpload() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(10, 5);
    AtomicInteger processedCount = new AtomicInteger(0);
    Set<String> threadNames = ConcurrentHashMap.newKeySet();

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      uploader.uploadBatches(
          batches,
          batch -> {
            threadNames.add(Thread.currentThread().getName());
            processedCount.incrementAndGet();
          });

      assertEquals(10, processedCount.get());
      // With parallelism=4 and 10 batches, we expect multiple threads to be used
      assertTrue(threadNames.size() > 1, "Expected multiple threads, but got: " + threadNames);
    }
  }

  @Test
  @DisplayName("test parallelism is forced to 1 when ordered writes enabled")
  void testOrderedWritesForcesSequential() {
    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, true)) {
      // Effective parallelism should be 1 when bulkWriteOrdered=true
      assertEquals(1, uploader.getEffectiveParallelism());
    }
  }

  @Test
  @DisplayName("test parallelism is preserved when unordered writes enabled")
  void testUnorderedWritesPreservesParallelism() {
    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      assertEquals(4, uploader.getEffectiveParallelism());
    }
  }

  @Test
  @DisplayName("test upload with empty batch list")
  void testEmptyBatchList() {
    List<List<MongoProcessedSinkRecordData>> batches = new ArrayList<>();
    AtomicInteger processedCount = new AtomicInteger(0);

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      uploader.uploadBatches(batches, batch -> processedCount.incrementAndGet());

      assertEquals(0, processedCount.get());
    }
  }

  @Test
  @DisplayName("test upload with single batch falls back to sequential")
  void testSingleBatchFallsBackToSequential() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(1, 10);
    Set<String> threadNames = ConcurrentHashMap.newKeySet();

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      uploader.uploadBatches(batches, batch -> threadNames.add(Thread.currentThread().getName()));

      assertEquals(1, threadNames.size());
    }
  }

  @Test
  @DisplayName("test graceful shutdown")
  void testGracefulShutdown() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(5, 10);
    ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false);

    uploader.uploadBatches(batches, batch -> {});

    // Close should not throw
    uploader.close();
  }

  @Test
  @DisplayName("test error propagation from batch processor")
  void testErrorPropagation() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(5, 10);

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      assertThrows(
          CompletionException.class,
          () ->
              uploader.uploadBatches(
                  batches,
                  batch -> {
                    throw new RuntimeException("Test error");
                  }));
    }
  }

  @Test
  @DisplayName("test all batches are processed even with parallelism")
  void testAllBatchesProcessed() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(20, 5);
    AtomicInteger totalRecordsProcessed = new AtomicInteger(0);

    try (ParallelBatchUploader uploader = new ParallelBatchUploader(4, 100, false)) {
      uploader.uploadBatches(batches, batch -> totalRecordsProcessed.addAndGet(batch.size()));

      // 20 batches * 5 records each = 100 total records
      assertEquals(100, totalRecordsProcessed.get());
    }
  }

  @Test
  @DisplayName("test sequential upload when ordered writes enabled with high parallelism config")
  void testOrderedWritesWithHighParallelismConfig() {
    List<List<MongoProcessedSinkRecordData>> batches = createBatches(10, 5);
    List<Integer> processingOrder = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger counter = new AtomicInteger(0);

    // Even with parallelism=8, bulkWriteOrdered=true should force sequential
    try (ParallelBatchUploader uploader = new ParallelBatchUploader(8, 100, true)) {
      assertEquals(1, uploader.getEffectiveParallelism());

      uploader.uploadBatches(batches, batch -> processingOrder.add(counter.getAndIncrement()));

      // With sequential processing, order should be 0, 1, 2, 3, ...
      for (int i = 0; i < processingOrder.size(); i++) {
        assertEquals(i, processingOrder.get(i));
      }
    }
  }

  private List<List<MongoProcessedSinkRecordData>> createBatches(
      final int numBatches, final int recordsPerBatch) {
    MongoSinkConfig sinkConfig = createSinkConfig();
    List<List<MongoProcessedSinkRecordData>> batches = new ArrayList<>();

    for (int b = 0; b < numBatches; b++) {
      List<MongoProcessedSinkRecordData> batch = new ArrayList<>();
      for (int r = 0; r < recordsPerBatch; r++) {
        SinkRecord record =
            new SinkRecord(
                TEST_TOPIC,
                0,
                Schema.STRING_SCHEMA,
                format("{_id: %s, a: %s}", r, r),
                Schema.STRING_SCHEMA,
                format("{_id: %s, a: %s}", r, r),
                r);
        batch.add(new MongoProcessedSinkRecordData(record, sinkConfig));
      }
      batches.add(batch);
    }
    return batches;
  }
}
