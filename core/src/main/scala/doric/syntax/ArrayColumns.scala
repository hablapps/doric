package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.LambdaFunction.identity

trait ArrayColumns {

  /**
    * Concatenates multiple array columns together into a single column.
    * @param cols the array columns, must be Arrays of the same type.
    * @tparam T The type of the elements of the arrays.
    * @return Doric Column with the concatenation.
    */
  def concatArrays[T](cols: DoricColumn[Array[T]]*): DoricColumn[Array[T]] =
    cols.toList.traverse(_.elem).map(f.concat(_: _*)).toDC

  def array[T](cols: DoricColumn[T]*): DoricColumn[Array[T]] =
    cols.toList.traverse(_.elem).map(f.array(_: _*)).toDC

  implicit class ArrayColumnSyntax[T](private val col: ArrayColumn[T]) {

    /**
      * Selects the nth element of the array, returns null value if the length is shorter than n.
      * @param n the index of the element to retreave.
      * @return the DoricColumn with the selected element.
      */
    def getIndex(n: Int): DoricColumn[T] =
      col.elem.map(_.apply(n)).toDC

    /**
      * Transform each element with the provided function.
      * @param fun lambda with the transformation to apply.
      * @tparam A the type of the array elements to return.
      * @return the column reference with the applied transformation.
      */
    def transform[A](
        fun: DoricColumn[T] => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      (col.elem, fun(x).elem)
        .mapN((a, f) => new Column(ArrayTransform(a.expr, lam1(f.expr))))
        .toDC

    /**
      * Transform each element of the array with the provided function that provides the index of the element in the array.
      * @param fun the lambda that takes in account the element of the array and the index and returns a new element.
      * @tparam A the type of the elements of the array
      * @return the column reference with the provided transformation.
      */
    def transformWithIndex[A](
        fun: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[Array[A]] =
      (col.elem, fun(x, y).elem).mapN { (a, f) =>
        new Column(ArrayTransform(a.expr, lam2(f.expr)))
      }.toDC

    /**
      * Aggregates (reduce) the array with the provided functions, similar to scala fold left in collections, with a final transformation.
      * @param zero zero value
      * @param merge function to combine the previous result with the element of the array
      * @param finish the final transformation
      * @tparam A type of the intermediate values
      * @tparam B type of the final value to return
      * @return the column reference with the applied transformation.
      */
    def aggregateWT[A, B](zero: DoricColumn[A])(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
        finish: DoricColumn[A] => DoricColumn[B]
    ): DoricColumn[B] =
      (col.elem, zero.elem, merge(x, y).elem, finish(x).elem).mapN {
        (a, z, m, f) =>
          new Column(ArrayAggregate(a.expr, z.expr, lam2(m.expr), lam1(f.expr)))
      }.toDC

    /**
      * Aggregates (reduce) the array with the provided functions, similar to scala fold left in collections.
      * @param zero zero value.
      * @param merge function to combine the previous result with the element of the array.
      * @tparam A type of the transformed values.
      * @return the column reference with the applied transformation.
      */
    def aggregate[A](
        zero: DoricColumn[A]
    )(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A]
    ): DoricColumn[A] =
      (col.elem, zero.elem, merge(x, y).elem).mapN { (a, z, m) =>
        new Column(ArrayAggregate(a.expr, z.expr, lam2(m.expr), identity))
      }.toDC

    /**
      * Filters the array elements using the provided condition.
      * @param p the condition to filter.
      * @return the column reference with the filter applied.
      */
    def filter(p: DoricColumn[T] => BooleanColumn): DoricColumn[Array[T]] =
      (col.elem, p(x).elem)
        .mapN((a, f) => new Column(ArrayFilter(a.expr, lam1(f.expr))))
        .toDC

    private def x[A]: DoricColumn[A] =
      DoricColumn.uncheckedTypeAndExistence[A](new Column(xarg))
    private def y[A]: DoricColumn[A] =
      DoricColumn.uncheckedTypeAndExistence[A](new Column(yarg))

    private def lam1(e: Expression) = LambdaFunction(e, Seq(xarg))
    private def lam2(e: Expression) = LambdaFunction(e, Seq(xarg, yarg))

    private val xarg = UnresolvedNamedLambdaVariable(Seq("x"))
    private val yarg = UnresolvedNamedLambdaVariable(Seq("y"))
  }
}
