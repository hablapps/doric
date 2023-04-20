package doric
package syntax

import doric.types.SparkType

private[syntax] trait ControlStructures {

  /**
    * Initialize a when builder
    * @group Control structure
    * @tparam T
    *   the type of the returnign DoricColumn
    * @return
    *   WhenBuilder instance to add the required logic.
    */
  def when[T]: WhenBuilder[T] = WhenBuilder()

  implicit class ControlStructuresImpl[O: SparkType](col: DoricColumn[O]) {
    def matches[T]: MatchBuilder[O, T] = MatchBuilder(col, WhenBuilder())
  }
}
