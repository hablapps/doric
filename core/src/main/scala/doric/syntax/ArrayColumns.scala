package doric
package syntax

import scala.jdk.CollectionConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag

import cats.data.Kleisli
import cats.implicits._
import doric.types.{CollectionType, LiteralSparkType, SparkType}

import org.apache.spark.sql.{Column, Dataset, Row, functions => f}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.LambdaFunction.identity

protected final case class Zipper[T1, T2, F[_]: CollectionType](
    col: DoricColumn[F[T1]],
    col2: DoricColumn[F[T2]]
) {
  def apply[O](
      f: (DoricColumn[T1], DoricColumn[T2]) => DoricColumn[O]
  ): DoricColumn[F[O]] = {
    val xv = x(col.getIndex(0))
    val yv = y(col2.getIndex(0))
    (
      col.elem,
      col2.elem,
      f(xv, yv).elem,
      xv.elem,
      yv.elem
    ).mapN { (a, b, f, x, y) =>
      new Column(ZipWith(a.expr, b.expr, lam2(f.expr, x.expr, y.expr)))
    }.toDC
  }
}

protected trait ArrayColumns {

  /**
    * Concatenates multiple array columns together into a single column.
    *
    * @group Array Type
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
    * @see org.apache.spark.sql.functions.array
    * @todo scaladoc link (issue #135)
    */
  def array[T: SparkType: ClassTag](
      cols: DoricColumn[T]*
  )(implicit lt: LiteralSparkType[Array[T]]): ArrayColumn[T] =
    if (cols.nonEmpty)
      cols.toList.traverse(_.elem).map(f.array(_: _*)).toDC
    else
      lit(Array.empty[T])

  /**
    * Creates a new list column. The input columns must all have the same data type.
    *
    * @group Array Type
    * @see org.apache.spark.sql.functions.array
    * @todo scaladoc link (issue #135)
    */
  def list[T](cols: DoricColumn[T]*): DoricColumn[List[T]] =
    cols.toList.traverse(_.elem).map(f.array(_: _*)).toDC

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
      * the index of the element to retrieve.
      * @return
      * the DoricColumn with the selected element.
      */
    def getIndex(n: Int): DoricColumn[T] =
      (col.elem, n.lit.elem)
        .mapN((a, b) => (a, b))
        .mapK(toEither)
        .flatMap { case (a, b) =>
          Kleisli[DoricEither, Dataset[_], Column]((df: Dataset[_]) => {
            new Column(
              ExtractValue(
                a.expr,
                b.expr,
                df.sparkSession.sessionState.analyzer.resolver
              )
            ).asRight
          })
        }
        .mapK(toValidated)
        .toDC

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
      * @see org.apache.spark.sql.functions.transform
      * @todo scaladoc link (issue #135)
      */
    def transform[A](
        fun: DoricColumn[T] => DoricColumn[A]
    ): DoricColumn[F[A]] = {
      val xv = x(col.getIndex(0))
      (col.elem, fun(xv).elem, xv.elem)
        .mapN((a, f, x) =>
          new Column(ArrayTransform(a.expr, lam1(f.expr, x.expr)))
        )
        .toDC
    }

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
      * @see org.apache.spark.sql.functions.transform
      * @todo scaladoc link (issue #135)
      */
    def transformWithIndex[A](
        fun: (DoricColumn[T], IntegerColumn) => DoricColumn[A]
    ): DoricColumn[F[A]] = {
      val xv = x(col.getIndex(0))
      val yv = y(1.lit)
      (
        col.elem,
        fun(xv, yv).elem,
        xv.elem,
        yv.elem
      ).mapN { (a, f, x, y) =>
        new Column(ArrayTransform(a.expr, lam2(f.expr, x.expr, y.expr)))
      }.toDC
    }

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
      * @see org.apache.spark.sql.functions.aggregate
      * @todo scaladoc link (issue #135)
      */
    def aggregateWT[A, B](zero: DoricColumn[A])(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A],
        finish: DoricColumn[A] => DoricColumn[B]
    ): DoricColumn[B] = {
      val xv = x(zero)
      val yv = y(col.getIndex(0))
      (
        col.elem,
        zero.elem,
        merge(xv, yv).elem,
        finish(xv).elem,
        xv.elem,
        yv.elem
      ).mapN { (a, z, m, f, x, y) =>
        new Column(
          ArrayAggregate(
            a.expr,
            z.expr,
            lam2(m.expr, x.expr, y.expr),
            lam1(f.expr, x.expr)
          )
        )
      }.toDC
    }

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
      * @see org.apache.spark.sql.functions.aggregate
      * @todo scaladoc link (issue #135)
      */
    def aggregate[A](
        zero: DoricColumn[A]
    )(
        merge: (DoricColumn[A], DoricColumn[T]) => DoricColumn[A]
    ): DoricColumn[A] = {
      val xv = x(zero)
      val yv = y(col.getIndex(0))
      (
        col.elem,
        zero.elem,
        merge(xv, yv).elem,
        xv.elem,
        yv.elem
      ).mapN { (a, z, m, x, y) =>
        new Column(
          ArrayAggregate(a.expr, z.expr, lam2(m.expr, x.expr, y.expr), identity)
        )
      }.toDC
    }

    /**
      * Filters the array elements using the provided condition.
      *
      * @group Array Type
      * @param p
      *   the condition to filter.
      * @return
      *   the column reference with the filter applied.
      * @see org.apache.spark.sql.functions.filter
      * @todo scaladoc link (issue #135)
      */
    def filter(p: DoricColumn[T] => BooleanColumn): DoricColumn[F[T]] = {
      val xv = x(col.getIndex(0))
      (col.elem, p(xv).elem, xv.elem)
        .mapN((a, f, x) =>
          new Column(ArrayFilter(a.expr, lam1(f.expr, x.expr)))
        )
        .toDC
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
      * @see org.apache.spark.sql.functions.array_join
      * @todo scaladoc link (issue #135)
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
      * @see org.apache.spark.sql.functions.array_join
      * @todo scaladoc link (issue #135)
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
      * @see [[https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html#array_sort(e:org.apache.spark.sql.Column):org.apache.spark.sql.Column]]
      */
    def sortAscNullsLast: DoricColumn[F[T]] = col.elem.map(f.array_sort).toDC

    /**
      * Sorts the input array for the given column in ascending order,
      * according to the natural ordering of the array elements.
      * Null elements will be placed at the beginning of the returned array.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.sort_array(e:org\.apache\.spark\.sql\.Column,asc* org.apache.spark.sql.functions.sort_array]]
      */
    def sortAscNullsFirst: DoricColumn[F[T]] = col.elem.map(f.sort_array).toDC

    /**
      * Sorts the input array for the given column in ascending or descending order,
      * according to the natural ordering of the array elements.
      * Null elements will be placed at the beginning of the returned array in ascending order or
      * at the end of the returned array in descending order.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.sort_array(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sort_array]]
      */
    def sort(asc: BooleanColumn): DoricColumn[F[T]] =
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
    def union(cols: DoricColumn[F[T]]*): DoricColumn[F[T]] =
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
    def overlaps(col2: DoricColumn[F[T]]): BooleanColumn =
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
      * @see org.apache.spark.sql.functions.exists
      * @todo scaladoc link not available for spark 2.4
      */
    def exists(fun: DoricColumn[T] => BooleanColumn): BooleanColumn = {
      val xv = x(col.getIndex(0))
      (col.elem, fun(xv).elem, xv.elem)
        .mapN((c, f, x) => {
          new Column(ArrayExists(c.expr, lam1(f.expr, x.expr)))
        })
        .toDC
    }

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
      * Creates a new row for each element with position in the given array column.
      *
      * @note Uses the default column name pos for position, and value for elements in the array
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *     ORIGINAL        SPARK         DORIC
      *  +------------+   +---+---+     +------+
      *  |col         |   |pos|col|     |col   |
      *  +------------+   +---+---+     +------+
      *  |[a, b, c, d]|   |0  |a  |     |{0, a}|
      *  |[e]         |   |1  |b  |     |{1, b}|
      *  |[]          |   |2  |c  |     |{2, c}|
      *  |null        |   |3  |d  |     |{3, d}|
      *  +------------+   |0  |e  |     |{0, e}|
      *                   +---+---+     +------+
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.posexplode]]
      */
    def posExplode: DoricColumn[Row] =
      col.zipWithIndex("pos".cname, "value".cname).elem.map(f.explode).toDC

    /**
      * Creates a new row for each element with position in the given array column.
      * Unlike posexplode, if the array is null or empty then the row null is produced.
      *
      * @note Uses the default column name pos for position, and col for elements in the array
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *     ORIGINAL        SPARK         DORIC
      *  +------------+   +----+----+     +------+
      *  |col         |   |pos |col |     |col   |
      *  +------------+   +----+----+     +------+
      *  |[a, b, c, d]|   |0   |a   |     |{0, a}|
      *  |[e]         |   |1   |b   |     |{1, b}|
      *  |[]          |   |2   |c   |     |{2, c}|
      *  |null        |   |3   |d   |     |{3, d}|
      *  +------------+   |0   |e   |     |{0, e}|
      *                   |null|null|     |null  |
      *                   |null|null|     |null  |
      *                   +----+----+     +------+
      * }}}
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.posexplode_outer]]
      */
    def posExplodeOuter: DoricColumn[Row] =
      col
        .zipWithIndex("pos".cname, "value".cname)
        .elem
        .map(f.explode_outer)
        .toDC

    /**
      * Returns an array with reverse order of elements.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.reverse]]
      */
    def reverse: DoricColumn[F[T]] = reverseAbstract(col)

    /**
      * Returns a random permutation of the given array.
      *
      * @note
      *   The function is non-deterministic.
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.shuffle]]
      */
    def shuffle: DoricColumn[F[T]] = col.elem.map(f.shuffle).toDC

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
      * if `start` == 0 an exception will be thrown
      * @group Array Type
      * @see org.apache.spark.sql.functions.slice
      * @todo scaladoc link (issue #135)
      */
    def slice(start: IntegerColumn, length: IntegerColumn): DoricColumn[F[T]] =
      (col.elem, start.elem, length.elem)
        .mapN((a, b, c) => new Column(Slice(a.expr, b.expr, c.expr)))
        .toDC

    /**
      * DORIC EXCLUSIVE! Given any array[e] column this method will return a new
      * array struct[i, e] column, where the first element is the index and
      * the second element is the value itself
      *
      * @group Array Type
      */
    def zipWithIndex(
        indexName: CName = "index".cname,
        valueName: CName = "value".cname
    ): DoricColumn[F[Row]] =
      col.transformWithIndex((value, index) =>
        struct(index.asCName(indexName), value.asCName(valueName))
      )

    /**
      * Merge two given arrays, element-wise, into a single array using a function.
      * If one array is shorter, nulls are appended at the end to match the length of the longer
      * array, before applying the function.
      *
      * @example {{{
      *   df.select(colArray("val1").zipWith(col("val2"), concat(_, _)))
      * }}}
      *
      * @group Array Type
      * @see org.apache.spark.sql.functions.zip_with
      * @todo scaladoc link not available for spark 2.4
      */
    def zipWith[T2](
        col2: DoricColumn[F[T2]]
    ): Zipper[T, T2, F] = {
      Zipper(col, col2)
    }

    /**
      * Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.arrays_zip]]
      */
    def zip(
        other: DoricColumn[F[T]],
        others: DoricColumn[F[T]]*
    ): DoricColumn[F[Row]] = {
      val cols = col +: (other +: others)
      cols.toList.traverse(_.elem).map(f.arrays_zip).toDC
    }

    /**
      * Creates a new map column.
      * The array in the first column is used for keys.
      * The array in the second column is used for values.
      *
      * @throws java.lang.RuntimeException if arrays doesn't have the same length.
      *                                    or if a key is null
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.map_from_arrays]]
      */
    def mapFromArrays[V](values: DoricColumn[F[V]]): MapColumn[T, V] =
      (col.elem, values.elem).mapN(f.map_from_arrays).toDC

    /**
      * Creates a new map column.
      * The array in the first column is used for keys.
      * The array in the second column is used for values.
      *
      * @throws java.lang.RuntimeException if arrays doesn't have the same length
      *                                    or if a key is null
      *
      * @group Array Type
      * @see [[mapFromArrays]]
      */
    def toMap[V](values: DoricColumn[F[V]]): MapColumn[T, V] =
      mapFromArrays(values)

    /**
      * Converts a column containing a StructType into a JSON string with the specified schema.
      * @throws java.lang.IllegalArgumentException in the case of an unsupported type.
      *
      * @group Array Type
      * @see org.apache.spark.sql.functions.to_json(e:org\.apache\.spark\.sql\.Column,options:scala\.collection\.immutable\.Map\[java\.lang\.String,java\.lang\.String\]):* org.apache.spark.sql.functions.to_csv
      * @todo scaladoc link (issue #135)
      */
    def toJson(options: Map[String, String] = Map.empty): StringColumn =
      col.elem.map(x => f.to_json(x, options.asJava)).toDC
  }

  /**
    * Extension methods for arrays
    *
    * @group Array Type
    */
  implicit class ArrayColumnTupleSyntax[K, V, F[_]: CollectionType](
      private val col: DoricColumn[F[(K, V)]]
  ) {

    /**
      * Returns a map created from the given array of entries.
      * All elements in the array for key should not be null.
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.map_from_entries]]
      */
    def mapFromEntries: MapColumn[K, V] = col.elem.map(f.map_from_entries).toDC

    @inline def toMap: MapColumn[K, V] = mapFromEntries

  }

  implicit class ArrayArrayColumnSyntax[G[_]: CollectionType, F[
      _
  ]: CollectionType, T](
      private val col: DoricColumn[F[G[T]]]
  ) {

    /**
      * Creates a single collection from an collection of collections.
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.flatten]]
      */
    def flatten: DoricColumn[F[T]] =
      col.elem.map(f.flatten).toDC
  }
}
