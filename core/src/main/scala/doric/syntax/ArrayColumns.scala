package doric
package syntax

import cats.implicits._
import doric.types.CollectionType

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.LambdaFunction.identity

private[syntax] trait ArrayColumns {

  /**
    * Concatenates multiple array columns together into a single column.
    *
    * @group Array Type
    * @param col
    *   the first array column, must be Arrays of the same type.
    * @param cols
    * the array columns, must be Arrays of the same type.
    * @tparam T
    * The type of the elements of the arrays.
    * @return
    *   Doric Column with the concatenation.
    * @see [[org.apache.spark.sql.functions.concat]]
    */
  def concatArrays[T, F[_]: CollectionType](
      cols: DoricColumn[F[T]]*
  ): DoricColumn[F[T]] =
    cols.toList.traverse(_.elem).map(f.concat(_: _*)).toDC

  /**
    * Creates a new array column. The input columns must all have the same data type.
    *
    * @group Array Type
    * @see [[org.apache.spark.sql.functions.array]]
    */
  def array[T](cols: DoricColumn[T]*): ArrayColumn[T] =
    cols.toList.traverse(_.elem).map(f.array(_: _*)).toDC

  /**
    * @group Array Type
    */
  def list[T](cols: DoricColumn[T]*): DoricColumn[List[T]] =
    cols.toList.traverse(_.elem).map(f.array(_: _*)).toDC

  /**
    * Returns a merged array of structs in which the N-th struct contains all N-th values of input
    * arrays.
    *
    * @group Array Type
    * @see [[org.apache.spark.sql.functions.arrays_zip]]
    */
  def zipArrays[T](col: ArrayColumn[T], cols: ArrayColumn[T]*): ArrayColumn[T]/*ArrayColumn[DStructColumn]*/ =
    (col +: cols).toList.traverse(_.elem).map(f.arrays_zip(_: _*)).toDC

  /**
    * Extension methods for arrays
    *
    * @group Array Type
    */
  implicit class ArrayColumnSyntax[T, F[_]: CollectionType](
      private val col: DoricColumn[F[T]]
  ) {

    /**
      * Selects the nth element of the array, returns null value if the length
      * is shorter than n.
      *
      * @group Array Type
      * @param n
      * the index of the element to retreave.
      * @return
      * the DoricColumn with the selected element.
      */
    def getIndex(n: Int): DoricColumn[T] =
      col.elem.map(_.apply(n)).toDC

    /**
      * Transform each element with the provided function.
      *
      * @group Array Type
      * @param fun
      *   lambda with the transformation to apply.
      * @tparam A
      *   the type of the array elements to return.
      * @return
      *   the column reference with the applied transformation.
      * @see [[org.apache.spark.sql.functions.transform]]
      */
    def transform[A](
        fun: DoricColumn[T] => DoricColumn[A]
    ): DoricColumn[F[A]] =
      (col.elem, fun(x).elem)
        .mapN((a, f) => new Column(ArrayTransform(a.expr, lam1(f.expr))))
        .toDC

    /**
      * Transform each element of the array with the provided function that
      * provides the index of the element in the array.
      *
      * @group Array Type
      * @param fun
      *   the lambda that takes in account the element of the array and the
      *   index and returns a new element.
      * @tparam A
      *   the type of the elements of the array
      * @return
      *   the column reference with the provided transformation.
      * @see [[org.apache.spark.sql.functions.transform]]
      */
    def transformWithIndex[A](
        fun: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[F[A]] =
      (col.elem, fun(x, y).elem).mapN { (a, f) =>
        new Column(ArrayTransform(a.expr, lam2(f.expr)))
      }.toDC

    /**
      * Aggregates (reduce) the array with the provided functions, similar to
      * scala fold left in collections, with a final transformation.
      *
      * @group Array Type
      * @param zero
      *   zero value
      * @param merge
      *   function to combine the previous result with the element of the array
      * @param finish
      *   the final transformation
      * @tparam A
      *   type of the intermediate values
      * @tparam B
      *   type of the final value to return
      * @return
      *   the column reference with the applied transformation.
      * @see [[org.apache.spark.sql.functions.aggregate]]
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
      * Aggregates (reduce) the array with the provided functions, similar to
      * scala fold left in collections.
      *
      * @group Array Type
      * @param zero
      *   zero value.
      * @param merge
      *   function to combine the previous result with the element of the array.
      * @tparam A
      *   type of the transformed values.
      * @return
      *   the column reference with the applied transformation.
      * @see [[org.apache.spark.sql.functions.aggregate]]
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
      *
      * @group Array Type
      * @param p
      *   the condition to filter.
      * @return
      *   the column reference with the filter applied.
      * @see [[org.apache.spark.sql.functions.filter]]
      */
    def filter(p: DoricColumn[T] => BooleanColumn): DoricColumn[F[T]] =
      (col.elem, p(x).elem)
        .mapN((a, f) => new Column(ArrayFilter(a.expr, lam1(f.expr))))
        .toDC

    /**
      * Returns an array of elements for which a predicate holds in a given array.
      * @example {{{
      *   df.select(filter(col("s"), (x, i) => i % 2 === 0))
      * }}}
      *
      * @param function
      *   (col, index) => predicate, the Boolean predicate to filter the input column
      *   given the index. Indices start at 0.
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.filter]]
      */
    def filterWIndex(
        function: (DoricColumn[T], IntegerColumn) => BooleanColumn
    ): ArrayColumn[T] = {
      (col.elem, function(x, y).elem).mapN { (a, f) =>
        new Column(ArrayFilter(a.expr, lam2(f.expr)))
      }.toDC
    }

    /**
      * Returns null if the array is null, true if the array contains `value`, and false otherwise.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_contains]]
      */
    def contains[A](value: DoricColumn[A]): BooleanColumn =
      (col.elem, value.elem)
        .mapN((c, v) => {
          new Column(ArrayContains(c.expr, v.expr))
        })
        .toDC

    /**
      * Removes duplicate values from the array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_distinct]]
      */
    def distinct: ArrayColumn[T] = col.elem.map(f.array_distinct).toDC

    /**
      * Returns an array of the elements in the first array but not in the second array,
      * without duplicates. The order of elements in the result is not determined
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_except]]
      */
    def except(col2: ArrayColumn[T]): ArrayColumn[T] =
      (col.elem, col2.elem).mapN(f.array_except).toDC

    /**
      * Returns an array of the elements in the intersection of the given two arrays,
      * without duplicates.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_intersect]]
      */
    def intersect(col2: ArrayColumn[T]): ArrayColumn[T] =
      (col.elem, col2.elem).mapN(f.array_intersect).toDC

    /**
      * Concatenates the elements of `column` using the `delimiter`. Null values are replaced with
      * `nullReplacement`.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_join]]
      */
    def join(
        delimiter: StringColumn,
        nullReplacement: StringColumn
    ): StringColumn =
      (col.elem, delimiter.elem, nullReplacement.elem)
        .mapN((c, d, n) => {
          new Column(ArrayJoin(c.expr, d.expr, Some(n.expr)))
        })
        .toDC

    /**
      * Concatenates the elements of `column` using the `delimiter`. Nulls are deleted
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_join]]
      */
    def join(delimiter: StringColumn): StringColumn =
      (col.elem, delimiter.elem)
        .mapN((c, d) => {
          new Column(ArrayJoin(c.expr, d.expr, None))
        })
        .toDC

    /**
      * Returns the maximum value in the array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_max]]
      */
    def max: DoricColumn[T] = col.elem.map(f.array_max).toDC

    /**
      * Returns the minimum value in the array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_min]]
      */
    def min: DoricColumn[T] = col.elem.map(f.array_min).toDC

    /**
      * Locates the position of the first occurrence of the value in the given array as long.
      * Returns null if either of the arguments are null.
      *
      * @note
      *   The position is not zero based, but 1 based index. Returns 0 if value could not be found in array.
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_position]]
      */
    def positionOf[B](col2: DoricColumn[B]): LongColumn =
      (col.elem, col2.elem)
        .mapN((c1, c2) => {
          new Column(ArrayPosition(c1.expr, c2.expr))
        })
        .toDC

    /**
      * Remove all elements that equal to element from the given array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_remove]]
      */
    def remove[B](col2: DoricColumn[B]): ArrayColumn[T] =
      (col.elem, col2.elem)
        .mapN((c1, c2) => {
          new Column(ArrayRemove(c1.expr, c2.expr))
        })
        .toDC

    /**
      * Sorts the input array in ascending order. The elements of the input array must be orderable.
      * Null elements will be placed at the end of the returned array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_sort]]
      */
    def sortAscNullsLast: ArrayColumn[T] = col.elem.map(f.array_sort).toDC

    /**
      * Sorts the input array for the given column in ascending order,
      * according to the natural ordering of the array elements.
      * Null elements will be placed at the beginning of the returned array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.sort_array]]
      */
    def sortAscNullsFirst: ArrayColumn[T] = col.elem.map(f.sort_array).toDC

    /**
      * Sorts the input array for the given column in ascending or descending order,
      * according to the natural ordering of the array elements.
      * Null elements will be placed at the beginning of the returned array in ascending order or
      * at the end of the returned array in descending order.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.sort_array]]
      */
    def sort(asc: BooleanColumn): ArrayColumn[T] =
      (col.elem, asc.elem)
        .mapN((c, a) => {
          new Column(SortArray(c.expr, a.expr))
        })
        .toDC

    /**
      * Returns an array of the elements in the union of the given N arrays, without duplicates.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.array_union]]
      */
    def union(cols: ArrayColumn[T]*): ArrayColumn[T] =
      (col +: cols).reduce((a, b) => {
        (a.elem, b.elem).mapN(f.array_union).toDC
      })

    /**
      * Returns `true` if `a1` and `a2` have at least one non-null element in common. If not and both
      * the arrays are non-empty and any of them contains a `null`, it returns `null`. It returns
      * `false` otherwise.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.arrays_overlap]]
      */
    def overlaps[B](col2: ArrayColumn[T]): BooleanColumn =
      (col.elem, col2.elem).mapN(f.arrays_overlap).toDC

    /**
      * Returns element of array at given index in value.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.element_at]]
      */
    def elementAt(pos: IntegerColumn): DoricColumn[T] =
      elementAtAbstract(col, pos)

    /**
      * Returns whether a predicate holds for one or more elements in the array.
      * @example {{{
      *   df.select(colArray("i").exists(_ % 2 === 0))
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.exists]]
      */
    def exists(fun: DoricColumn[T] => BooleanColumn): BooleanColumn =
      (col.elem, fun(x).elem)
        .mapN((c, f) => {
          new Column(ArrayExists(c.expr, lam1(f.expr)))
        })
        .toDC

    /**
      * Creates a new row for each element in the given array column.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.explode]]
      */
    def explode: DoricColumn[T] = col.elem.map(f.explode).toDC

    /**
      * Creates a new row for each element in the given array column.
      * Unlike explode, if the array is null or empty then null is produced.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.explode_outer]]
      */
    def explodeOuter: DoricColumn[T] = col.elem.map(f.explode_outer).toDC

    /**
      * Creates a single array from an array of arrays. If a structure of nested arrays is deeper than
      * two levels, only one level of nesting is removed.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.flatten]]
      */
    def flatten[B]: ArrayColumn[B] = col.elem.map(f.flatten).toDC

    /**
      * Returns whether a predicate holds for every element in the array.
      * @example {{{
      *   df.select(colArray("i").forAll(x => x % 2 === 0))
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.forall]]
      */
    def forAll(fun: DoricColumn[T] => BooleanColumn): BooleanColumn =
      (col.elem, fun(x).elem)
        .mapN((c, f) => {
          new Column(ArrayForAll(c.expr, lam1(f.expr)))
        })
        .toDC

    /**
      * Returns an array with reverse order of elements.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.reverse]]
      */
    def reverse: ArrayColumn[T] = reverseAbstract(col)

    /**
      * Returns a random permutation of the given array.
      *
      * @note
      *   The function is non-deterministic.
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.shuffle]]
      */
    def shuffle: ArrayColumn[T] = col.elem.map(f.shuffle).toDC

    /**
      * Returns length of array.
      *
      * The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
      * spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
      * With the default settings, the function returns -1 for null input.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.size]]
      */
    def size: IntegerColumn = col.elem.map(f.size).toDC

    /**
      * Returns an array containing all the elements in the column from index `start` (or starting from the
      * end if `start` is negative) with the specified `length`.
      *
      * @note
      *   if `start` == 0 an exception will be thrown
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.slice]]
      */
    def slice(start: IntegerColumn, length: IntegerColumn): ArrayColumn[T] =
      (col.elem, start.elem, length.elem).mapN(f.slice).toDC

    /**
      * Merge two given arrays, element-wise, into a single array using a function.
      * If one array is shorter, nulls are appended at the end to match the length of the longer
      * array, before applying the function.
      * @example {{{
      *   df.select(colArray("val1").zipWith(col("val2"), concat(_, _)))
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.zip_with]]
      */
    def zipWith(
        col2: ArrayColumn[T],
        function: (DoricColumn[T], DoricColumn[T]) => DoricColumn[T]
    ): ArrayColumn[T] = {
      (col.elem, col2.elem, function(x, y).elem).mapN { (a, b, f) =>
        new Column(ZipWith(a.expr, b.expr, lam2(f.expr)))
      }.toDC
    }
  }
}
