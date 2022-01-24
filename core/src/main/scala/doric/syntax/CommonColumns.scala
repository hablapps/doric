package doric
package syntax

import cats.implicits._
import doric.sem.Location
import doric.types.{Casting, SparkType, UnsafeCasting}
import org.apache.spark.sql.catalyst.expressions.ArrayRepeat
import org.apache.spark.sql.{Column, functions => f}

private[syntax] trait CommonColumns extends ColGetters[NamedDoricColumn] {

  /**
    * Returns the first column that is not null, or null if all inputs are null.
    *
    * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a
    * is null and b is not null, or c if both a and b are null but c is not
    * null.
    *
    * @group All Types
    * @param cols
    *   the DoricColumns to coalesce
    * @return
    *   the first column that is not null, or null if all inputs are null.
    * @see [[org.apache.spark.sql.functions.coalesce]]
    */
  def coalesce[T](cols: DoricColumn[T]*): DoricColumn[T] =
    cols.map(_.elem).toList.sequence.map(f.coalesce(_: _*)).toDC

  /**
    * Calculates the hash code of given columns, and returns the result as an integer column.
    *
    * @group All Types
    * @see [[org.apache.spark.sql.functions.hash]]
    */
  def hash(cols: DoricColumn[_]*): IntegerColumn =
    cols.map(_.elem).toList.sequence.map(f.hash(_: _*)).toDC

  /**
    * Calculates the hash code of given columns using the 64-bit
    * variant of the xxHash algorithm, and returns the result as a long column.
    *
    * @group All Types
    * @see [[org.apache.spark.sql.functions.xxhash64]]
    */
  def xxhash64(cols: DoricColumn[_]*): LongColumn =
    cols.map(_.elem).toList.sequence.map(f.xxhash64(_: _*)).toDC

  /**
    * Returns the least value of the list of values, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @note skips null values
    * @group All Types
    * @see [[org.apache.spark.sql.functions.least(exprs* org.apache.spark.sql.functions.least]]
    */
  def least[T](col: DoricColumn[T], cols: DoricColumn[T]*): DoricColumn[T] =
    (col +: cols).map(_.elem).toList.sequence.map(f.least(_: _*)).toDC

  /**
    * Returns the greatest value of the list of values, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @note skips null values
    * @group All Types
    * @see [[org.apache.spark.sql.functions.greatest(exprs* org.apache.spark.sql.functions.greatest]]
    */
  def greatest[T](col: DoricColumn[T], cols: DoricColumn[T]*): DoricColumn[T] =
    (col +: cols).map(_.elem).toList.sequence.map(f.greatest(_: _*)).toDC

  override protected def constructSide[T](
      column: Doric[Column],
      colName: String
  ): NamedDoricColumn[T] =
    NamedDoricColumn(column, colName)

  implicit class SparkCol(private val column: Column) {

    /**
      * Allows to transform any spark `Column` reference to a DoricColumn
      * @param location
      *   the location if there is a error
      * @tparam T
      *   The expected type that should have the column.
      * @return
      *   A DoricColumn reference of the provided type T
      */
    @inline def asDoric[T: SparkType](implicit
        location: Location
    ): DoricColumn[T] =
      DoricColumn(column)
  }

  /**
    * Extension methods for any kind of column
    * @group All Types
    */
  implicit class BasicCol[T: SparkType](private val column: DoricColumn[T]) {

    private type CastToT[To]  = Casting[T, To]
    private type WCastToT[To] = UnsafeCasting[T, To]

    /**
      * Gives the column an alias.
      *
      * @group All Types
      * @param colName
      * the alias to set the column.
      * @return
      * DoricColumn with the alias
      */
    def as(colName: String): NamedDoricColumn[T] =
      NamedDoricColumn[T](column, colName)

    /**
      * Gives the column an alias.
      *
      * @group All Types
      * @param colName
      * the alias to set the column.
      * @return
      * DoricColumn with the alias
      */
    def asCName(colName: CName): NamedDoricColumn[T] =
      NamedDoricColumn[T](column, colName.value)

    /**
      * Type safe equals between Columns
      *
      * @group All Types
      * @param other
      * the column to compare
      * @return
      * a reference to a Boolean DoricColumn with the comparation
      */
    def ===(other: DoricColumn[T]): BooleanColumn =
      (column.elem, other.elem).mapN(_ === _).toDC

    /**
      * Type safe distinct between Columns
      * @group All Types
      * @param other
      *   the column to compare
      * @return
      *   a reference to a Boolean DoricColumn with the comparation
      */
    def =!=(other: DoricColumn[T]): BooleanColumn =
      (column.elem, other.elem).mapN(_ =!= _).toDC

    /**
      * Pipes the column with the provided transformation
      * @group All Types
      * @param f
      *   the function to apply to the column.
      * @tparam O
      *   the returning type
      * @return
      *   the DoricColumn reference with of the provided logic
      */
    def pipe[O](f: DoricColumn[T] => DoricColumn[O]): DoricColumn[O] = f(column)

    /**
      * Cast the column.
      * @group All Types
      * @tparam To
      *   the type to cast to.
      * @return
      *   the DoricColumn of the provided type.
      */
    def cast[To: CastToT: SparkType]: DoricColumn[To] =
      Casting[T, To].cast(column)

    /**
      * Allows to cast to posible wrong or with unexpected behaviour type, like
      * casting String to Int, that can be resulted in null types.
      * @group All Types
      * @tparam To
      *   the type to cast to.
      * @return
      *   the DoricColumn of the provided type
      */
    def unsafeCast[To: WCastToT: SparkType]: DoricColumn[To] =
      UnsafeCasting[T, To].cast(column)

    /**
      * Checks if the element is equal to any of the provided literals.
      * @group All Types
      * @param elems
      *   literals to compare to
      * @return
      *   Boolean DoricColumn with the comparation logic.
      * @see [[org.apache.spark.sql.Column.isin]]
      */
    def isIn(elems: T*): BooleanColumn = column.elem.map(_.isin(elems: _*)).toDC

    /**
      * Checks if the value of the column is null
      * @group All Types
      * @return
      *   Boolean DoricColumn
      * @see [[org.apache.spark.sql.Column.isNull]]
      */
    def isNull: BooleanColumn = column.elem.map(_.isNull).toDC

    /**
      * Checks if the value of the column is not null
      * @group All Types
      * @return
      *   Boolean DoricColumn
      * @see [[org.apache.spark.sql.Column.isNotNull]]
      */
    def isNotNull: BooleanColumn = column.elem.map(_.isNotNull).toDC

    /**
      * Creates an array containing the left argument repeated the number of times given by the
      * right argument.
      *
      * @group All Types
      * @see [[org.apache.spark.sql.functions.array_repeat(left* org.apache.spark.sql.functions.array_repeat]]
      */
    def repeatArray(times: IntegerColumn): ArrayColumn[T] =
      (column.elem, times.elem)
        .mapN((c1, c2) => {
          new Column(ArrayRepeat(c1.expr, c2.expr))
        })
        .toDC

  }

}
