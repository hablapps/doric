package habla.doric
package syntax

import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame, functions => f}
import org.apache.spark.sql.catalyst.expressions._

trait ArrayColumnOps {

  type DoricEither[A] = Either[NonEmptyChain[Throwable], A]

  private val toValidated = new FunctionK[DoricEither, DoricValidated] {
    override def apply[A](fa: DoricEither[A]): DoricValidated[A] = fa.toValidated
  }
  private val toEither = new FunctionK[DoricValidated, DoricEither] {
    override def apply[A](fa: DoricValidated[A]): DoricEither[A] = fa.toEither
  }

  implicit class ArrayColumnSyntax[T](private val col: ArrayColumn[T]) {

    def getIndex(n: Int): DoricColumn[T] = {
      col.elem.map(_.apply(n)).toDC
    }

    private val xarg = UnresolvedNamedLambdaVariable(Seq("x"))
    private val yarg = UnresolvedNamedLambdaVariable(Seq("y"))

    def transform[A](f: DoricColumn[T] => DoricColumn[A]): DoricColumn[Array[A]] =
      (col.elem, f(DoricColumn[T](new Column(xarg))).elem).mapN { (arr, fun) =>
        new Column(ArrayTransform(arr.expr, LambdaFunction(fun.expr, Seq(xarg))))
      }.toDC

    def transformWithIndex[A](
        f: (DoricColumn[T], IntegerColumn) => DoricColumn[A]): DoricColumn[Array[A]] =
      (col.elem, f(
          DoricColumn[T](new Column(xarg)),
          DoricColumn[Int](new Column(yarg))).elem).mapN { (arr, fun) =>
        new Column(ArrayTransform(arr.expr, LambdaFunction(fun.expr, Seq(xarg, yarg))))
      }.toDC

    def aggregate[A, B](
        initialValue: DoricColumn[A],
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
        finish: DoricColumn[A] => DoricColumn[B]): DoricColumn[B] =
      (col.elem,
       initialValue.elem,
       merge(DoricColumn[A](new Column(xarg)), DoricColumn[T](new Column(yarg))).elem,
       finish(DoricColumn[A](new Column(xarg))).elem).mapN { (arr, init, mrg, fin) =>
        new Column(ArrayAggregate(
          arr.expr,
          init.expr,
          LambdaFunction(mrg.expr, Seq(xarg, yarg)),
          LambdaFunction(fin.expr, Seq(xarg))))
      }.toDC

    def aggregate[A](initialValue: DoricColumn[A])(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A]): DoricColumn[A] =
      (col.elem, initialValue.elem, merge(
          DoricColumn[A](new Column(xarg)),
          DoricColumn[T](new Column(yarg))).elem).mapN { (arr, init, fun) =>
        new Column(ArrayAggregate(
          arr.expr,
          init.expr,
          LambdaFunction(fun.expr, Seq(xarg, yarg)),
          LambdaFunction.identity))
      }.toDC

    def filter(f: DoricColumn[T] => BooleanColumn): DoricColumn[Array[T]] =
      (col.elem, f(DoricColumn[T](new Column(xarg))).elem).mapN { (arr, fun) =>
        new Column(ArrayFilter(arr.expr, LambdaFunction(fun.expr, Seq(xarg))))
      }.toDC
  }
}

