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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.primitives.Ints;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class GroupedAggregator {
  private final GroupedAccumulator accumulator;
  private final AggregationNode.Step step;
  private final TSDataType outputType;
  private final int[] inputChannels;
  private final OptionalInt maskChannel;

  public GroupedAggregator(
      GroupedAccumulator accumulator,
      AggregationNode.Step step,
      TSDataType outputType,
      List<Integer> inputChannels,
      OptionalInt maskChannel) {
    this.accumulator = requireNonNull(accumulator, "accumulator is null");
    this.step = requireNonNull(step, "step is null");
    this.outputType = requireNonNull(outputType, "intermediateType is null");
    this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
    this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
    checkArgument(
        step.isInputRaw() || inputChannels.size() == 1,
        "expected 1 input channel for intermediate aggregation");
  }

  public TSDataType getType() {
    return outputType;
  }

  public void processBlock(int groupCount, int[] groupIds, TsBlock block) {
    accumulator.setGroupCount(groupCount);
    Column[] arguments = block.getColumns(inputChannels);

    // process count(*)
    if (arguments.length == 0) {
      arguments =
          new Column[] {new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, block.getPositionCount())};
    }

    if (step.isInputRaw()) {
      // Use select-all AggregationMask here because filter of Agg-Function is not supported now
      AggregationMask mask = AggregationMask.createSelectAll(block.getPositionCount());
      if (maskChannel.isPresent()) {
        mask.applyMaskBlock(block.getColumn(maskChannel.getAsInt()));
      }
      accumulator.addInput(groupIds, arguments, mask);
    } else {
      accumulator.addIntermediate(groupIds, arguments[0]);
    }
  }

  public void evaluate(int groupId, ColumnBuilder columnBuilder) {
    if (step.isOutputPartial()) {
      accumulator.evaluateIntermediate(groupId, columnBuilder);
    } else {
      accumulator.evaluateFinal(groupId, columnBuilder);
    }
  }

  public void prepareFinal() {
    accumulator.prepareFinal();
  }

  public void reset() {
    accumulator.reset();
  }

  public long getEstimatedSize() {
    return accumulator.getEstimatedSize();
  }

  public void close() {
    accumulator.close();
  }
}
