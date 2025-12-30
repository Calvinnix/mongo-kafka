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
package com.mongodb.kafka.connect.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

class SplitEventReassemblerTest {

  private SplitEventReassembler reassembler;

  @BeforeEach
  void setUp() {
    reassembler = new SplitEventReassembler();
  }

  @Test
  @DisplayName("Non-split documents should pass through unchanged")
  void testNonSplitDocument() {
    BsonDocument doc =
        new BsonDocument()
            .append("_id", new BsonString("123"))
            .append("operationType", new BsonString("insert"))
            .append("fullDocument", new BsonDocument("name", new BsonString("test")));

    BsonDocument result = reassembler.process(doc);

    assertNotNull(result);
    assertEquals(doc, result);
    assertFalse(reassembler.isCollecting());
  }

  @Test
  @DisplayName("Two-fragment event should be reassembled correctly")
  void testTwoFragmentReassembly() {
    // Fragment 1 of 2
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append("operationType", new BsonString("update"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)))
            .append("field1", new BsonString("value1"));

    BsonDocument result1 = reassembler.process(fragment1);
    assertNull(result1, "First fragment should return null");
    assertTrue(reassembler.isCollecting(), "Should be collecting fragments");

    // Fragment 2 of 2
    BsonDocument fragment2 =
        new BsonDocument()
            .append("_id", new BsonString("token2"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(2)))
            .append("field2", new BsonString("value2"));

    BsonDocument result2 = reassembler.process(fragment2);
    assertNotNull(result2, "Second fragment should return reassembled document");
    assertFalse(reassembler.isCollecting(), "Should not be collecting after reassembly");

    // Verify reassembled document
    assertFalse(
        result2.containsKey("splitEvent"), "Reassembled document should not have splitEvent field");
    assertEquals("token1", result2.getString("_id").getValue());
    assertEquals("update", result2.getString("operationType").getValue());
    assertEquals("value1", result2.getString("field1").getValue());
    assertEquals("value2", result2.getString("field2").getValue());
  }

  @Test
  @DisplayName("Three-fragment event should be reassembled correctly")
  void testThreeFragmentReassembly() {
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(3)))
            .append("field1", new BsonString("value1"));

    BsonDocument fragment2 =
        new BsonDocument()
            .append("_id", new BsonString("token2"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(3)))
            .append("field2", new BsonString("value2"));

    BsonDocument fragment3 =
        new BsonDocument()
            .append("_id", new BsonString("token3"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(3))
                    .append("of", new BsonInt32(3)))
            .append("field3", new BsonString("value3"));

    assertNull(reassembler.process(fragment1));
    assertTrue(reassembler.isCollecting());

    assertNull(reassembler.process(fragment2));
    assertTrue(reassembler.isCollecting());

    BsonDocument result = reassembler.process(fragment3);
    assertNotNull(result);
    assertFalse(reassembler.isCollecting());

    // Verify all fields are present
    assertEquals("value1", result.getString("field1").getValue());
    assertEquals("value2", result.getString("field2").getValue());
    assertEquals("value3", result.getString("field3").getValue());
    assertFalse(result.containsKey("splitEvent"));
  }

  @Test
  @DisplayName("Out-of-order fragments should reset collection")
  void testOutOfOrderFragments() {
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(3)))
            .append("field1", new BsonString("value1"));

    assertNull(reassembler.process(fragment1));
    assertTrue(reassembler.isCollecting());

    // Skip fragment 2, send fragment 3
    BsonDocument fragment3 =
        new BsonDocument()
            .append("_id", new BsonString("token3"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(3))
                    .append("of", new BsonInt32(3)))
            .append("field3", new BsonString("value3"));

    assertNull(reassembler.process(fragment3));
    assertFalse(reassembler.isCollecting(), "Should reset after out-of-order fragment");
  }

  @Test
  @DisplayName("Non-split document during fragment collection should reset and return document")
  void testNonSplitDocumentDuringCollection() {
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)))
            .append("field1", new BsonString("value1"));

    assertNull(reassembler.process(fragment1));
    assertTrue(reassembler.isCollecting());

    // Send a non-split document
    BsonDocument normalDoc =
        new BsonDocument()
            .append("_id", new BsonString("token2"))
            .append("operationType", new BsonString("insert"))
            .append("fullDocument", new BsonDocument("name", new BsonString("test")));

    BsonDocument result = reassembler.process(normalDoc);
    assertNotNull(result);
    assertEquals(normalDoc, result);
    assertFalse(reassembler.isCollecting(), "Should reset after receiving non-split document");
  }

  @Test
  @DisplayName("Overlapping fields in fragments should use later fragment values")
  void testOverlappingFields() {
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)))
            .append("sharedField", new BsonString("value1"));

    BsonDocument fragment2 =
        new BsonDocument()
            .append("_id", new BsonString("token2"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(2)))
            .append("sharedField", new BsonString("value2"));

    assertNull(reassembler.process(fragment1));
    BsonDocument result = reassembler.process(fragment2);

    assertNotNull(result);
    assertEquals(
        "value2",
        result.getString("sharedField").getValue(),
        "Later fragment should override earlier values");
  }

  @Test
  @DisplayName("Reset should clear all state")
  void testReset() {
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(2)))
            .append("field1", new BsonString("value1"));

    assertNull(reassembler.process(fragment1));
    assertTrue(reassembler.isCollecting());

    reassembler.reset();
    assertFalse(reassembler.isCollecting());
  }

  @Test
  @DisplayName("Fragments can be collected across multiple batches")
  void testCrossBatchFragmentCollection() {
    // Simulate first batch: Fragment 1 arrives
    BsonDocument fragment1 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(1))
                    .append("of", new BsonInt32(3)))
            .append("data1", new BsonString("value1"));

    BsonDocument result1 = reassembler.process(fragment1);
    assertNull(result1, "Fragment 1 should not return a document");
    assertTrue(reassembler.isCollecting(), "Should be collecting after fragment 1");

    // Simulate batch boundary - reassembler state persists
    // Second batch: Fragment 2 arrives
    BsonDocument fragment2 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(2))
                    .append("of", new BsonInt32(3)))
            .append("data2", new BsonString("value2"));

    BsonDocument result2 = reassembler.process(fragment2);
    assertNull(result2, "Fragment 2 should not return a document");
    assertTrue(reassembler.isCollecting(), "Should still be collecting after fragment 2");

    // Third batch: Fragment 3 arrives
    BsonDocument fragment3 =
        new BsonDocument()
            .append("_id", new BsonString("token1"))
            .append(
                "splitEvent",
                new BsonDocument()
                    .append("fragment", new BsonInt32(3))
                    .append("of", new BsonInt32(3)))
            .append("data3", new BsonString("value3"));

    BsonDocument result3 = reassembler.process(fragment3);
    assertNotNull(result3, "Fragment 3 should return reassembled document");
    assertFalse(reassembler.isCollecting(), "Should not be collecting after reassembly");

    // Verify reassembled document has all fields
    assertTrue(result3.containsKey("data1"));
    assertTrue(result3.containsKey("data2"));
    assertTrue(result3.containsKey("data3"));
    assertFalse(result3.containsKey("splitEvent"), "splitEvent should be removed");
    assertEquals("value1", result3.getString("data1").getValue());
    assertEquals("value2", result3.getString("data2").getValue());
    assertEquals("value3", result3.getString("data3").getValue());
  }
}
