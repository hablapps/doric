package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._

object BebeFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)

  def bebe_regexp_extract_all(col: Column, regex: Column, groupIndex: Column): Column = withExpr {
    RegExpExtractAll(col.expr, regex.expr, groupIndex.expr)
  }

}
