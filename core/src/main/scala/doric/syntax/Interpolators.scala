package doric
package syntax

trait Interpolators {
  implicit class doricStringInterpolator(val sc: StringContext) {
    def ds(columns: StringColumn*): StringColumn = {
      val literals: Seq[StringColumn] = sc.parts.map(_.lit)

      val cols: Seq[StringColumn] = literals
        .zipAll(columns, "".lit, "".lit)
        .flatMap { case (a, b) =>
          List(a, b)
        }

      concatWs("".lit, cols: _*)
    }
  }
}
