package org.apache.iotdb.db.mpp.plan.statement.sys;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

import java.util.List;

public class ExplainLogicalPlanStatement extends Statement {

  private final QueryStatement queryStatement;

  public ExplainLogicalPlanStatement(QueryStatement queryStatement) {
    this.queryStatement = queryStatement;
  }

  public QueryStatement getQueryStatement() {
    return queryStatement;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return queryStatement.getPaths();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitExplainLogicalPlan(this, context);
  }
}
