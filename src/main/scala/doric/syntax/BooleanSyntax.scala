package doric
package syntax

import doric.DoricColumn.sparkFunction

trait BooleanSyntax {

  implicit class BooleanOperationsSyntax(
      column: DoricColumn[Boolean]
  ) {
    def and(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ && _)
    def &&(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ && _)
    def or(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ || _)
    def ||(other: DoricColumn[Boolean]): DoricColumn[Boolean] =
      sparkFunction(column, other, _ || _)
  }
}
