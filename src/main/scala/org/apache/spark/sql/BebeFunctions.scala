package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._

/**
 * @groupname string_funcs String Functions
 */
object BebeFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)

  /**
   * Extract all strings in the `str` that match the `regexp` expression
   * and corresponding to the regex group index.
   * @group string_funcs
   * @since 0.0.1
   */
  def bebe_regexp_extract_all(col: Column, regex: Column, groupIndex: Column): Column = withExpr {
    RegExpExtractAll(col.expr, regex.expr, groupIndex.expr)
  }

}
