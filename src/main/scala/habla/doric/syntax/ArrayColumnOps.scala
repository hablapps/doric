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
      col.elem
        .mapK(toEither)
        .flatMap(subC => {
          Kleisli[DoricEither, DataFrame, Column](df => {
            val untypedFunc: Column => DoricEither[Column] = (c: Column) =>
              func(DoricColumn[T](c)).elem
                .run(df)
                .toEither
            val x                           = UnresolvedNamedLambdaVariable(Seq("x"))
            val result: DoricEither[Column] = untypedFunc(new Column(x))
            result.map(_ =>
              f.transform(
                subC,
                x => {
                  untypedFunc(x).right.get
                }
              )
            )
          })
        })
        .mapK(toValidated)
        .toDC

    def doricFunc[A,B](f: DoricColumn[A] => DoricColumn[B]): Doric[Column => Column] = {
      val x                           = UnresolvedNamedLambdaVariable(Seq("x"))
      f(DoricColumn[A](new Column(x))).elem.mapK(toEither)
      .flatMap(_ => Kleisli[DoricEither, DataFrame, Column => Column](df => ((x: Column) => f(DoricColumn[A](x)).elem.run(df).toEither.right.get).asRight))
        .mapK(toValidated)
    }


    def transformWithIndex[A](
        func: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      col.elem
        .mapK(toEither)
        .flatMap(subC => {
          Kleisli[DoricEither, DataFrame, Column](df => {
            val untypedFunc: (Column, Column) => DoricEither[Column] = (c: Column, c2: Column) =>
              func(DoricColumn[T](c), DoricColumn[Int](c2)).elem
                .run(df)
                .toEither
            val x                           = UnresolvedNamedLambdaVariable(Seq("x"))
            val y                           = UnresolvedNamedLambdaVariable(Seq("y"))
            val result: DoricEither[Column] = untypedFunc(new Column(x), new Column(y))
            result.map(_ =>
              f.transform(
                subC,
                (x, y) => {
                  untypedFunc(x, y).right.get
                }
              )
            )
          })
        })
        .mapK(toValidated)
        .toDC

  }

}
