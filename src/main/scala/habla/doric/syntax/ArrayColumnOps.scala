package habla.doric
package syntax

import cats.arrow.FunctionK
import cats.data.{Kleisli, NonEmptyChain}
import cats.implicits._

import org.apache.spark.sql.{Column, DataFrame, functions => f}
import org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable

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

    def transform[A](func: DoricColumn[T] => DoricColumn[A]): DoricColumn[Array[A]] =
      (col.elem, doricFunc(func))
        .mapN((c, untypedFunc) => f.transform(c, untypedFunc))
        .toDC

    private def doricFunc[A, B](f: DoricColumn[A] => DoricColumn[B]): Doric[Column => Column] = {
      val x = UnresolvedNamedLambdaVariable(Seq("x"))
      f(DoricColumn[A](new Column(x))).elem
        .mapK(toEither)
        .flatMap(_ =>
          Kleisli[DoricEither, DataFrame, Column => Column](df =>
            ((x: Column) => f(DoricColumn[A](x)).elem.run(df).toEither.right.get).asRight
          )
        )
        .mapK(toValidated)
    }

    private def doricFunc2[A1, A2, B](
        f: (DoricColumn[A1], DoricColumn[A2]) => DoricColumn[B]
    ): Doric[(Column, Column) => Column] = {
      val x = UnresolvedNamedLambdaVariable(Seq("x"))
      val y = UnresolvedNamedLambdaVariable(Seq("y"))
      f(DoricColumn[A1](new Column(x)), DoricColumn[A2](new Column(y))).elem
        .mapK(toEither)
        .flatMap(_ =>
          Kleisli[DoricEither, DataFrame, (Column, Column) => Column](df =>
            (
                (
                    x: Column,
                    y: Column
                ) => f(DoricColumn[A1](x), DoricColumn[A2](y)).elem.run(df).toEither.right.get
            ).asRight
          )
        )
        .mapK(toValidated)
    }

    def transformWithIndex[A](
        func: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      (col.elem, doricFunc2(func)).mapN((c, untypedFunc) => f.transform(c, untypedFunc)).toDC

    def aggregate[A, B](
        initialValue: DoricColumn[A],
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
        finish: DoricColumn[A] => DoricColumn[B]
    ): DoricColumn[B] =
      (col.elem, initialValue.elem, doricFunc2(merge), doricFunc(finish)).mapN(f.aggregate).toDC

    def aggregate[A](initialValue: DoricColumn[A])(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A]
    ): DoricColumn[A] =
      (col.elem, initialValue.elem, doricFunc2(merge)).mapN(f.aggregate).toDC

    def filter(func: DoricColumn[T] => BooleanColumn): DoricColumn[Array[T]] =
      (col.elem, doricFunc(func)).mapN((c, funt) => f.filter(c, funt)).toDC
  }

}
