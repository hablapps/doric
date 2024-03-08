package doric
package syntax

import cats.implicits._
import doric.types.CollectionType

import org.apache.spark.sql.{Column, Row, functions => f}
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
      * @see org.apache.spark.sql.functions.filter
      * @todo scaladoc link (issue #135)
      */
    def filterWIndex(
        function: (DoricColumn[T], IntegerColumn) => BooleanColumn
    ): ArrayColumn[T] = {
      val xv = x(col.getIndex(0))
      val yv = y(1.lit)
      (
        col.elem,
        function(xv, yv).elem,
        xv.elem,
        yv.elem
      ).mapN { (a, f, x, y) =>
        new Column(ArrayFilter(a.expr, lam2(f.expr, x.expr, y.expr)))
      }.toDC
    }

    /**
      * Sorts the input array based on the given comparator function. The comparator will take two
      * arguments representing two elements of the array. It returns a negative integer, 0, or a
      * positive integer as the first element is less than, equal to, or greater than the second
      * element.
      *
      * @example {{{
      * colArrayString("myColumn").sortBy((l, r) => when[Int]
      *    .caseW(l.length > r.length, 1.lit)
      *    .caseW(l.length < r.length, (-1).lit)
      *    .otherwise(0.lit)
      * )
      * }}}
      *
      * @note If the comparator function returns null, the function will fail and raise an error.
      *
      * @group Array Type
      */
    def sortBy(
        fun: (DoricColumn[T], DoricColumn[T]) => IntegerColumn
    ): ArrayColumn[T] = {
      val xv = x(col.getIndex(0))
      val yv = y(col.getIndex(1))

      (col.elem, fun(xv, yv).elem, xv.elem, yv.elem)
        .mapN((c, f, x, y) => {
          new Column(ArraySort(c.expr, lam2(f.expr, x.expr, y.expr)))
        })
        .toDC
    }

    /**
      * Sorts the input array based on the given comparator function. The comparator will take two
      * arguments representing two elements of the array. It returns a negative integer, 0, or a
      * positive integer as the first element is less than, equal to, or greater than the second
      * element.
      *
      * @example {{{
      * colArrayString("myColumn").sortBy(c => c.length)
      * }}}
      *
      * @note If the comparator function returns null, the function will fail and raise an error.
      *
      * @group Array Type
      */
    def sortBy[A](fun: DoricColumn[T] => DoricColumn[A]): ArrayColumn[T] = {
      val xv = x(col.getIndex(0))
      val yv = y(col.getIndex(1))

      (col.elem, fun(xv).elem, fun(yv).elem, xv.elem, yv.elem)
        .mapN((c, fx, fy, x, y) =>
          new Column(
            ArraySort(
              c.expr,
              lam2(ArraySort.comparator(fx.expr, fy.expr), x.expr, y.expr)
            )
          )
        )
        .toDC
    }
  }

  implicit class ArrayStructColumnSyntax3x[F[_]: CollectionType](
      private val arrRowCol: DoricColumn[F[Row]]
  ) {

    /**
      * Sorts the input array **of structs** based on the given column names and orders.
      * It accepts also CName objects, which will be applied the default order
      *
      * @example {{{
      * colArray[Row]("myColumn").sortBy(
      *   // implicit conversion + default order
      *   CName("column1"),
      *
      *   // default order
      *   CNameOrd("column2"),
      *
      *   // Custom order
      *   CNameOrd("column3", DescNullsLast)
      * )
      * }}}
      *
      * @group Array Type
      */
    def sortBy(
        ordCol: CNameOrd,
        ordCols: CNameOrd*
    ): ArrayColumn[Row] = {
      val xv = x(arrRowCol.getIndex(0))
      val yv = y(arrRowCol.getIndex(1))

      val << : Byte   = -1
      val >> : Byte   = 1
      val areEq: Byte = 0

      (arrRowCol.elem, xv.elem, yv.elem)
        .mapN((c, x, y) => {

          val getComparator: CNameOrd => Column = orderedCol => {
            val left  = x.getField(orderedCol.name.value)
            val right = y.getField(orderedCol.name.value)

            val comparator: (Column, Column) => Column =
              (l, r) => new Column(ArraySort.comparator(l.expr, r.expr))

            def whenNullCond(
                leftIsNull: Byte,
                rightIsNull: Byte,
                otherwise: Column
            ): Column =
              f.when(left.isNull and right.isNotNull, leftIsNull)
                .when(left.isNotNull and right.isNull, rightIsNull)
                .otherwise(otherwise)

            orderedCol.order match {
              case Asc | AscNullsLast =>
                comparator(left, right) // Default behaviour
              case AscNullsFirst =>
                whenNullCond(
                  leftIsNull = <<,
                  rightIsNull = >>,
                  otherwise = comparator(left, right)
                )
              case Desc | DescNullsFirst =>
                comparator(right, left)
              case DescNullsLast =>
                whenNullCond(
                  leftIsNull = >>,
                  rightIsNull = <<,
                  otherwise = comparator(right, left)
                )
            }
          }

          val initialComparator = getComparator(ordCol)
          val initial = f.when(initialComparator =!= areEq, initialComparator)

          val allColsComparator = ordCols
            .foldLeft(initial)((sortOpts, currCol) => {
              val comparator = getComparator(currCol)
              sortOpts.when(comparator =!= areEq, comparator)
            })
            .otherwise(areEq)

          new Column(
            ArraySort(
              c.expr,
              lam2(allColsComparator.expr, x.expr, y.expr)
            )
          )
        })
        .toDC
    }
  }
}
