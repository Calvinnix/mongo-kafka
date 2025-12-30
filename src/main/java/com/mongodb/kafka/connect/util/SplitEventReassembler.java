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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Handles reassembly of split change stream events.
 *
 * <p>When MongoDB's $changeStreamSplitLargeEvent stage splits an event that exceeds 16 MB, it
 * creates multiple fragments. Each fragment contains a "splitEvent" field with:
 *
 * <ul>
 *   <li>fragment: the current fragment number (1-based)
 *   <li>of: the total number of fragments
 * </ul>
 *
 * <p>This class collects all fragments and reassembles them into a single complete document.
 */
public final class SplitEventReassembler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplitEventReassembler.class);
  private static final String SPLIT_EVENT_FIELD = "splitEvent";
  private static final String FRAGMENT_FIELD = "fragment";
  private static final String OF_FIELD = "of";

  private final List<BsonDocument> fragments = new ArrayList<>();
  private int expectedFragmentCount = 0;
  private boolean isCollecting = false;

  /**
   * Processes a document that may be a fragment of a split event.
   *
   * @param document the document to process
   * @return the reassembled document if all fragments have been collected, or null if more
   *     fragments are needed
   */
  public BsonDocument process(final BsonDocument document) {
    if (!isSplitFragment(document)) {
      // Not a split fragment, return as-is
      reset();
      return document;
    }

    BsonDocument splitEvent = document.getDocument(SPLIT_EVENT_FIELD);
    int fragmentNumber = splitEvent.getInt32(FRAGMENT_FIELD).getValue();
    int totalFragments = splitEvent.getInt32(OF_FIELD).getValue();

    if (fragmentNumber == 1) {
      // First fragment - start collecting
      reset();
      isCollecting = true;
      expectedFragmentCount = totalFragments;
      fragments.add(document);
      LOGGER.debug("Started collecting split event fragments: 1 of {}", totalFragments);
      return null; // Need more fragments
    } else if (isCollecting && fragmentNumber == fragments.size() + 1) {
      // Next expected fragment
      fragments.add(document);
      LOGGER.debug("Collected fragment {} of {}", fragmentNumber, totalFragments);

      if (fragments.size() == expectedFragmentCount) {
        // All fragments collected - reassemble
        BsonDocument reassembled = reassemble(fragments);
        LOGGER.debug("Reassembled {} fragments into complete document", expectedFragmentCount);
        reset();
        return reassembled;
      }
      return null; // Need more fragments
    } else {
      // Unexpected fragment order or missing fragments
      LOGGER.warn(
          "Unexpected fragment order: received fragment {} but expected {}. Resetting fragment collection.",
          fragmentNumber,
          fragments.size() + 1);
      reset();
      return null;
    }
  }

  /**
   * Checks if there are fragments currently being collected.
   *
   * @return true if fragments are being collected
   */
  public boolean isCollecting() {
    return isCollecting;
  }

  /** Resets the reassembler state. */
  public void reset() {
    fragments.clear();
    expectedFragmentCount = 0;
    isCollecting = false;
  }

  private boolean isSplitFragment(final BsonDocument document) {
    return document.containsKey(SPLIT_EVENT_FIELD) && document.get(SPLIT_EVENT_FIELD).isDocument();
  }

  private BsonDocument reassemble(final List<BsonDocument> fragments) {
    if (fragments.isEmpty()) {
      return new BsonDocument();
    }

    // Start with a new document and copy fields from the first fragment
    BsonDocument reassembled = new BsonDocument();
    BsonDocument firstFragment = fragments.get(0);
    for (String key : firstFragment.keySet()) {
      if (!key.equals(SPLIT_EVENT_FIELD)) {
        reassembled.put(key, firstFragment.get(key));
      }
    }

    // Merge fields from subsequent fragments
    for (int i = 1; i < fragments.size(); i++) {
      BsonDocument fragment = fragments.get(i);
      for (String key : fragment.keySet()) {
        if (!key.equals(SPLIT_EVENT_FIELD) && !key.equals("_id")) {
          // Merge the field, with later fragments taking precedence
          BsonValue value = fragment.get(key);
          if (value != null) {
            reassembled.put(key, value);
          }
        }
      }
    }

    return reassembled;
  }
}
