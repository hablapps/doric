package habla.doric
package syntax

import cats.implicits._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.LambdaFunction.identity

trait ArrayColumnOps {

  implicit class ArrayColumnSyntax[T](private val col: ArrayColumn[T]) {

    def getIndex(n: Int): DoricColumn[T] =
      col.elem.map(_.apply(n)).toDC

    def transform[A](
        fun: DoricColumn[T] => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      (col.elem, fun(x).elem)
        .mapN((a, f) => new Column(ArrayTransform(a.expr, lam1(f.expr))))
        .toDC

    def transformWithIndex[A](
        fun: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      (col.elem, fun(x, y).elem).mapN { (a, f) =>
        new Column(ArrayTransform(a.expr, lam2(f.expr)))
      }.toDC

    def aggregateWT[A, B](zero: DoricColumn[A])(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
        finish: DoricColumn[A] => DoricColumn[B]
    ): DoricColumn[B] =
      (col.elem, zero.elem, merge(x, y).elem, finish(x).elem).mapN {
        (a, z, m, f) =>
          new Column(ArrayAggregate(a.expr, z.expr, lam2(m.expr), lam1(f.expr)))
      }.toDC

    def aggregate[A](
        zero: DoricColumn[A]
    )(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A]
    ): DoricColumn[A] =
      (col.elem, zero.elem, merge(x, y).elem).mapN { (a, z, m) =>
        new Column(ArrayAggregate(a.expr, z.expr, lam2(m.expr), identity))
      }.toDC

    def filter(p: DoricColumn[T] => BooleanColumn): DoricColumn[Array[T]] =
      (col.elem, p(x).elem)
        .mapN((a, f) => new Column(ArrayFilter(a.expr, lam1(f.expr))))
        .toDC

    private def x[A]: DoricColumn[A] =
      DoricColumn.unchecked[A](new Column(xarg))
    private def y[A]: DoricColumn[A] =
      DoricColumn.unchecked[A](new Column(yarg))

    private def lam1(e: Expression) = LambdaFunction(e, Seq(xarg))
    private def lam2(e: Expression) = LambdaFunction(e, Seq(xarg, yarg))

    private val xarg = UnresolvedNamedLambdaVariable(Seq("x"))
    private val yarg = UnresolvedNamedLambdaVariable(Seq("y"))
  }
}
