package doric
package syntax

import doric.DoricColumn.sparkFunction

import org.apache.spark.sql.{functions => f}

private[syntax] trait BooleanColumns {

  /**
    * Inversion of boolean expression, i.e. NOT.
    *
    * @group Boolean Type
    * @see [[org.apache.spark.sql.functions.not]]
    */
  def not(col: BooleanColumn): BooleanColumn = col.elem.map(f.not).toDC

  /**
    * Inversion of boolean expression, i.e. NOT.
    *
    * @group Boolean Type
    * @see [[not]]
    */
  @inline def !(col: BooleanColumn): BooleanColumn = not(col)

  /**
    * @group Boolean Type
    */
  implicit class BooleanOperationsSyntax(
      column: DoricColumn[Boolean]
  ) {

    /**
      * Boolean AND
      *
      * @group Boolean Type
      */
    def and(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ && _)

    /**
      * Boolean AND
      *
      * @group Boolean Type
      */
    def &&(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      and(other)

    /**
      * Boolean OR
      *
      * @group Boolean Type
      */
    def or(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ || _)

    /**
      * Boolean OR
      *
      * @group Boolean Type
      */
    def ||(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      or(other)
  }
}
