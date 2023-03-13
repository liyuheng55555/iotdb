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

package org.apache.iotdb.db.mpp.plan.expression.ternary;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;

import java.nio.ByteBuffer;

public class CaseWhenThenExpression extends TernaryExpression {
  boolean isRoot; // if isRoot, the string will be "CASE WHEN...", else it will be "WHEN..."

  public CaseWhenThenExpression(
      Expression whenExpression,
      Expression thenExpression,
      Expression elseExpression,
      boolean isRoot) {
    super(whenExpression, thenExpression, elseExpression);
    this.isRoot = isRoot;
  }

  protected CaseWhenThenExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public CaseWhenThenExpression() {
    super(null, null, null);
  }

  public Expression getWhen() {
    return firstExpression;
  }

  public Expression getThen() {
    return secondExpression;
  }

  public Expression getElse() {
    return thirdExpression;
  }

  public void setRoot(boolean root) {
    this.isRoot = root;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.CASE_WHEN_THEN;
  }

  @Override
  protected String getExpressionStringInternal() {
    StringBuilder builder = new StringBuilder();
    if (isRoot) {
      builder.append("CASE ");
    }
    builder.append("WHEN ");
    builder.append(this.getWhen().getExpressionString()).append(" ");
    builder.append("THEN ");
    builder.append(this.getThen().getExpressionString()).append(" ");
    if (isRoot) {
      builder.append("ELSE ");
    }
    builder.append(this.getElse().getExpressionString());
    if (isRoot) {
      builder.append(" END");
    }
    return builder.toString();
  }

  // shouldn't use this method
  @Override
  protected String operator() {
    return "CWT";
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitCaseWhenThenExpression(this, context);
  }
}
