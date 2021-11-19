package doric

import cats.implicits._
import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{ElementAt, Expression, LambdaFunction, UnresolvedNamedLambdaVariable}

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
  @inline def reverseAbstract[T](dc: DoricColumn[T]): DoricColumn[T] =
    dc.elem.map(f.reverse).toDC

  @inline def x[A]: DoricColumn[A] =
    DoricColumn.uncheckedTypeAndExistence[A](new Column(xarg))
  @inline def y[A]: DoricColumn[A] =
    DoricColumn.uncheckedTypeAndExistence[A](new Column(yarg))
  @inline def z[A]: DoricColumn[A] =
    DoricColumn.uncheckedTypeAndExistence[A](new Column(zarg))

  @inline def lam1(e: Expression): LambdaFunction = LambdaFunction(e, Seq(xarg))
  @inline def lam2(e: Expression): LambdaFunction =
    LambdaFunction(e, Seq(xarg, yarg))
  @inline def lam3(e: Expression): LambdaFunction =
    LambdaFunction(e, Seq(xarg, yarg, zarg))

  private val xarg = UnresolvedNamedLambdaVariable(Seq("x"))
  private val yarg = UnresolvedNamedLambdaVariable(Seq("y"))
  private val zarg = UnresolvedNamedLambdaVariable(Seq("z"))
}
