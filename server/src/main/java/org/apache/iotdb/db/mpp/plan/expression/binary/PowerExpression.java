package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;

import java.nio.ByteBuffer;

public class PowerExpression extends ArithmeticBinaryExpression {

  public PowerExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public PowerExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.POWER;
  }

  @Override
  protected String operator() {
    return "^";
  }
}
