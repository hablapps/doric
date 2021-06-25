package doric
package syntax

import doric.DoricColumn.sparkFunction

trait BooleanColumns {

  implicit class BooleanOperationsSyntax(
      column: DoricColumn[Boolean]
  ) {

    /**
      * Boolean AND
      */
    def and(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ && _)

    /**
      * Boolean AND
      */
    def &&(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ && _)

    /**
      * Boolean OR
      */
    def or(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ || _)

    /**
      * Boolean OR
      */
    def ||(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ || _)
  }
}
