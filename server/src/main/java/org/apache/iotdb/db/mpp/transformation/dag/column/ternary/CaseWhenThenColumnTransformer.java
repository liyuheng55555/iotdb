/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.transformation.dag.column.ternary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.BooleanType;
import org.apache.iotdb.tsfile.read.common.type.DoubleType;
import org.apache.iotdb.tsfile.read.common.type.FloatType;
import org.apache.iotdb.tsfile.read.common.type.IntType;
import org.apache.iotdb.tsfile.read.common.type.LongType;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public class CaseWhenThenColumnTransformer extends TernaryColumnTransformer {

//  private List<WhenColumnTransformer> whenList;
//
//  private List<ThenColumnTransformer> thenList;
//
//  private ColumnBuilder resultBuilder;


  public CaseWhenThenColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  public ColumnTransformer getWhen() {
    return firstColumnTransformer;
  }

  public ColumnTransformer getThen() {
    return secondColumnTransformer;
  }

  public ColumnTransformer getElse() {
    return thirdColumnTransformer;
  }

  private void writeToColumnBuilder(ColumnTransformer childTransformer, Column column, int index, ColumnBuilder builder) {
    if (returnType instanceof BooleanType) {
      builder.writeBoolean(childTransformer.getType().getBoolean(column, index));
    }
    else if (returnType instanceof IntType) {
      builder.writeInt(childTransformer.getType().getInt(column, index));
    }
    else if (returnType instanceof LongType) {
      builder.writeLong(childTransformer.getType().getLong(column, index));
    }
    else if (returnType instanceof FloatType) {
      builder.writeFloat(childTransformer.getType().getFloat(column, index));
    }
    else if (returnType instanceof DoubleType) {
      builder.writeDouble(childTransformer.getType().getDouble(column, index));
    }
    else if (returnType instanceof BinaryType) {
      builder.writeBinary(childTransformer.getType().getBinary(column, index));
    }
    else {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  @Override
  protected void evaluate() {
    getWhen().tryEvaluate();
    getThen().tryEvaluate();
    getElse().tryEvaluate();
    int positionCount = getWhen().getColumnCachePositionCount();
    ColumnBuilder columnBuilder = returnType.createColumnBuilder(positionCount);
    Column whenColumn = getWhen().getColumn();
    Column thenColumn = getThen().getColumn();
    Column elseColumn = getElse().getColumn();
    for (int i = 0; i < positionCount; i++) {
      if (!whenColumn.isNull(i) && whenColumn.getBoolean(i)) {
        if (thenColumn.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          writeToColumnBuilder(getThen(), thenColumn, i, columnBuilder);
        }
      } else {
        if (elseColumn.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          writeToColumnBuilder(getElse(), elseColumn, i, columnBuilder);
        }
      }
    }
    initializeColumnCache(columnBuilder.build());
  }

  @Override
  protected void checkType() {
    if (getWhen().typeNotEquals(TypeEnum.BOOLEAN)){
      throw new UnsupportedOperationException("Unsupported Type");
    }
    if (getElse().getType() != null && !getThen().getType().getClass().equals(getElse().getType().getClass())) {
      throw new UnsupportedOperationException("CASE expression must have same type.");
    }
  }
}
