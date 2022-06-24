package doric
package syntax

import cats.implicits._
import doric.types.CollectionType

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._

trait ArrayColumns3x {

  implicit class ArrayColumnSyntax3x[T, F[_]: CollectionType](
      private val col: DoricColumn[F[T]]
  ) {

    /**
      * Returns whether a predicate holds for every element in the array.
      *
      * @example {{{
      *   df.select(colArray("i").forAll(x => x % 2 === 0))
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.forall]]
      */
    def forAll(fun: DoricColumn[T] => BooleanColumn): BooleanColumn = {
      val xv = x(col.getIndex(0))
      (col.elem, fun(xv).elem, xv.elem)
        .mapN((c, f, x) => {
          new Column(ArrayForAll(c.expr, lam1(f.expr, x.expr)))
        })
        .toDC
    }
  }
}
