package doric

import cats.implicits._
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{ElementAt, Expression, ExprId, LambdaFunction, NamedExpression, NamedLambdaVariable}

package object syntax {

  /**
    * Abstract method for Array Columns and Map Columns
    *
    * Returns element of array at given index in value if column is array. Returns value for
    * the given key in value if column is map.
    *
    * @param dc doric column where the item is to be searched
    * @param key doric column
    * @tparam T type of the doric column where the item is to be searched
    * @tparam K type of "key" doric column to perform the search
    * @tparam V type of "value" doric column result
    */
  @inline def elementAtAbstract[T, K, V](
      dc: DoricColumn[T],
      key: DoricColumn[K]
  ): DoricColumn[V] = {
    (dc.elem, key.elem)
      .mapN((c, k) => {
        new Column(ElementAt(c.expr, k.expr))
      })
      .toDC
  }

  /**
    * Abstract method for Array Columns and String Columns
    *
    * Returns a reversed string or an array with reverse order of elements.
    *
    * @param dc doric column to be reversed
    * @tparam T type of doric column (string or array)
    */
  @inline def reverseAbstract[T](
      dc: DoricColumn[T]
  ): DoricColumn[T] =
    dc.elem.map(f.reverse).toDC

  @inline private[syntax] def value[A](
      name: String,
      dc: DoricColumn[A]
  ): DoricColumn[A] = {
    val exprId: ExprId              = NamedExpression.newExprId
    val value: AtomicReference[Any] = new AtomicReference()
    dc.elem
      .map(c =>
        new Column(
          NamedLambdaVariable(
            name,
            c.expr.dataType,
            c.expr.nullable,
            exprId,
            value
          )
        )
      )
      .toDC
  }

  @inline private[syntax] def x[A](dc: DoricColumn[A]): DoricColumn[A] =
    value("x", dc)
  @inline private[syntax] def y[A](dc: DoricColumn[A]): DoricColumn[A] =
    value("y", dc)
  @inline private[syntax] def z[A](dc: DoricColumn[A]): DoricColumn[A] =
    value("z", dc)

  @inline private[syntax] def lam1(
      e: Expression,
      xarg: Expression
  ): LambdaFunction =
    LambdaFunction(e, Seq(xarg.asInstanceOf[NamedLambdaVariable]))
  @inline private[syntax] def lam2(
      e: Expression,
      xarg: Expression,
      yarg: Expression
  ): LambdaFunction =
    LambdaFunction(
      e,
      Seq(
        xarg.asInstanceOf[NamedLambdaVariable],
        yarg.asInstanceOf[NamedLambdaVariable]
      )
    )
  @inline private[syntax] def lam3(
      e: Expression,
      xarg: Expression,
      yarg: Expression,
      zarg: Expression
  ): LambdaFunction =
    LambdaFunction(
      e,
      Seq(
        xarg.asInstanceOf[NamedLambdaVariable],
        yarg.asInstanceOf[NamedLambdaVariable],
        zarg.asInstanceOf[NamedLambdaVariable]
      )
    )
}
