/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.cache.dualkeycache;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheUpdating;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl.DualKeyCachePolicy;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class DualKeyCacheTest {

  private final String policy;

  public DualKeyCacheTest(String policy) {
    this.policy = policy;
  }

  @Parameterized.Parameters
  public static List<String> getTestModes() {
    return Arrays.asList("FIFO", "LRU");
  }

  @Test
  public void testBasicReadPut() {
    DualKeyCacheBuilder<String, String, String> dualKeyCacheBuilder = new DualKeyCacheBuilder<>();
    IDualKeyCache<String, String, String> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(300)
            .firstKeySizeComputer(this::computeStringSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(this::computeStringSize)
            .build();

    String[] firstKeyList = new String[] {"root.db.d1", "root.db.d2"};
    String[] secondKeyList = new String[] {"s1", "s2"};
    String[][] valueTable =
        new String[][] {new String[] {"1-1", "1-2"}, new String[] {"2-1", "2-2"}};

    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        dualKeyCache.put(firstKeyList[i], secondKeyList[j], valueTable[i][j]);
      }
    }

    int firstKeyOfMissingEntry = -1, secondKeyOfMissingEntry = -1;
    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        String value = dualKeyCache.get(firstKeyList[i], secondKeyList[j]);
        if (value == null) {
          if (firstKeyOfMissingEntry == -1) {
            firstKeyOfMissingEntry = i;
            secondKeyOfMissingEntry = j;
          } else {
            Assert.fail();
          }
        } else {
          Assert.assertEquals(valueTable[i][j], value);
        }
      }
    }

    Assert.assertEquals(230, dualKeyCache.stats().memoryUsage());
    Assert.assertEquals(4, dualKeyCache.stats().requestCount());
    Assert.assertEquals(3, dualKeyCache.stats().hitCount());

    dualKeyCache.put(
        firstKeyList[firstKeyOfMissingEntry],
        secondKeyList[secondKeyOfMissingEntry],
        valueTable[firstKeyOfMissingEntry][secondKeyOfMissingEntry]);
    Assert.assertEquals(230, dualKeyCache.stats().memoryUsage());

    for (int i = 0; i < firstKeyList.length; i++) {
      int finalI = i;
      dualKeyCache.compute(
          new IDualKeyCacheComputation<String, String, String>() {
            @Override
            public String getFirstKey() {
              return firstKeyList[finalI];
            }

            @Override
            public String[] getSecondKeyList() {
              return secondKeyList;
            }

            @Override
            public void computeValue(int index, String value) {
              if (value != null) {
                Assert.assertEquals(valueTable[finalI][index], value);
              }
            }
          });
    }

    Assert.assertEquals(8, dualKeyCache.stats().requestCount());
    Assert.assertEquals(6, dualKeyCache.stats().hitCount());
  }

  private int computeStringSize(String string) {
    return 8 + 8 + 4 + 2 * string.length();
  }

  @Test
  public void testComputeAndUpdateSize() {
    final DualKeyCacheBuilder<String, String, String> dualKeyCacheBuilder =
        new DualKeyCacheBuilder<>();
    final IDualKeyCache<String, String, String> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.valueOf(policy))
            .memoryCapacity(500)
            .firstKeySizeComputer(this::computeStringSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(this::computeStringSize)
            .build();
    final String firstKey = "db";
    final String[] secondKeyList = new String[] {"root.db.d1", "root.db.d2"};
    for (final String s : secondKeyList) {
      dualKeyCache.put(firstKey, s, "");
    }
    int expectedSize =
        computeStringSize("db") + computeStringSize("root.db.d1") * 2 + computeStringSize("") * 2;
    Assert.assertEquals(expectedSize, dualKeyCache.stats().memoryUsage());
    dualKeyCache.updateWithLock(
        new IDualKeyCacheUpdating<String, String, String>() {
          @Override
          public String getFirstKey() {
            return firstKey;
          }

          @Override
          public String[] getSecondKeyList() {
            return secondKeyList;
          }

          @Override
          public int updateValue(final int index, final String value) {
            return computeStringSize("b") - computeStringSize("");
          }
        });
    expectedSize += (computeStringSize("b") - computeStringSize("")) * 2;
    Assert.assertEquals(expectedSize, dualKeyCache.stats().memoryUsage());
  }
}
