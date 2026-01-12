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
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ParallelRecordProcessorTest {

  @Test
  @DisplayName("test sequential processing when parallelism is 1")
  void testSequentialProcessing() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    List<SinkRecord> records = createSinkRecordList(TEST_TOPIC, 10);

    try (ParallelRecordProcessor processor = new ParallelRecordProcessor(1, 100, sinkConfig)) {
      List<MongoProcessedSinkRecordData> result = processor.process(records);

      assertEquals(10, result.size());
      // Verify order is preserved
      for (int i = 0; i < result.size(); i++) {
        assertEquals(i + 1, result.get(i).getSinkRecord().kafkaOffset());
      }
    }
  }

  @Test
  @DisplayName("test parallel processing preserves order")
  void testParallelProcessingPreservesOrder() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    List<SinkRecord> records = createSinkRecordList(TEST_TOPIC, 100);

    try (ParallelRecordProcessor processor = new ParallelRecordProcessor(4, 100, sinkConfig)) {
      List<MongoProcessedSinkRecordData> result = processor.process(records);

      assertEquals(100, result.size());
      // Verify order is preserved even with parallel processing
      for (int i = 0; i < result.size(); i++) {
        assertEquals(i + 1, result.get(i).getSinkRecord().kafkaOffset());
      }
    }
  }

  @Test
  @DisplayName("test fallback to sequential for small batches")
  void testFallbackToSequentialForSmallBatches() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    // Create fewer records than parallelism
    List<SinkRecord> records = createSinkRecordList(TEST_TOPIC, 2);

    try (ParallelRecordProcessor processor = new ParallelRecordProcessor(4, 100, sinkConfig)) {
      List<MongoProcessedSinkRecordData> result = processor.process(records);

      assertEquals(2, result.size());
      // Verify order is preserved
      for (int i = 0; i < result.size(); i++) {
        assertEquals(i + 1, result.get(i).getSinkRecord().kafkaOffset());
      }
    }
  }

  @Test
  @DisplayName("test processing with empty collection")
  void testEmptyCollection() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    List<SinkRecord> records = new ArrayList<>();

    try (ParallelRecordProcessor processor = new ParallelRecordProcessor(4, 100, sinkConfig)) {
      List<MongoProcessedSinkRecordData> result = processor.process(records);

      assertTrue(result.isEmpty());
    }
  }

  @Test
  @DisplayName("test graceful shutdown")
  void testGracefulShutdown() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    ParallelRecordProcessor processor = new ParallelRecordProcessor(4, 100, sinkConfig);

    // Process some records
    List<SinkRecord> records = createSinkRecordList(TEST_TOPIC, 10);
    List<MongoProcessedSinkRecordData> result = processor.process(records);
    assertEquals(10, result.size());

    // Close should not throw
    processor.close();
  }

  @Test
  @DisplayName("test processed records contain valid data")
  void testProcessedRecordsContainValidData() {
    MongoSinkConfig sinkConfig = createSinkConfig();
    List<SinkRecord> records = createSinkRecordList(TEST_TOPIC, 5);

    try (ParallelRecordProcessor processor = new ParallelRecordProcessor(2, 100, sinkConfig)) {
      List<MongoProcessedSinkRecordData> result = processor.process(records);

      for (MongoProcessedSinkRecordData processed : result) {
        assertNotNull(processed.getSinkRecord());
        assertNotNull(processed.getConfig());
        assertNotNull(processed.getNamespace());
        assertNotNull(processed.getWriteModel());
      }
    }
  }

  private List<SinkRecord> createSinkRecordList(final String topic, final int numRecords) {
    return IntStream.rangeClosed(1, numRecords)
        .boxed()
        .map(
            i ->
                new SinkRecord(
                    topic,
                    0,
                    Schema.STRING_SCHEMA,
                    format("{_id: %s, a: %s}", i, i),
                    Schema.STRING_SCHEMA,
                    format("{_id: %s, a: %s}", i, i),
                    i))
        .collect(toList());
  }
}
