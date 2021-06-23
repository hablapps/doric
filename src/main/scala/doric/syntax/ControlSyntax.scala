package doric
package syntax

import doric.control.WhenBuilder

trait ControlSyntax {
  def when[T]: WhenBuilder[T] = WhenBuilder()
}
