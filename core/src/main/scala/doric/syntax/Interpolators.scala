package doric
package syntax

trait Interpolators {
  implicit class doricStringInterpolator(val sc: StringContext) {
    def ds(columns: StringColumn*): StringColumn = {
      val literals: Seq[DoricColumn[String]] = sc.parts.map(_.lit)

      val cols: Seq[DoricColumn[String]] = literals
        .zipAll(columns, "".lit, "".lit)
        .flatMap { case (a, b) =>
          List(a, b)
        }

      concat(cols: _*)
    }
  }
}
