package doric
package syntax

import cats.implicits._
import doric.sem.Location
import doric.types.{Casting, SparkType, UnsafeCasting}

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.types.DataType

private[syntax] trait CommonColumns extends ColGetters[DoricColumn] {

  /**
    * Returns the spark `DataType` of the provided type
    * @tparam T
    *   the type to check
    * @return
    *   the spark `DataType`
    */
  @inline def dataType[T: SparkType]: DataType = SparkType[T].dataType

  /**
    * Returns the first column that is not null, or null if all inputs are null.
    *
    * For example, `coalesce(a, b, c)` will return a if a is not null, or b if a
    * is null and b is not null, or c if both a and b are null but c is not
    * null.
    *
    * @param cols
    *   the String DoricColumns to coalesce
    * @return
    *   the first column that is not null, or null if all inputs are null.
    */
  def coalesce[T](cols: DoricColumn[T]*): DoricColumn[T] =
    cols.map(_.elem).toList.sequence.map(f.coalesce(_: _*)).toDC

  override protected def constructSide[T](
      column: Doric[Column],
      colName: CName
  ): DoricColumn[T] =
    DoricColumn(column)

  implicit class SparkCol(private val column: Column) {

    /**
      * Allows to transform any spark `Column` reference to a DoricColumn
      * @param location
      *   the location if there is a error
      * @tparam T
      *   The expected type that should have the column.
      * @return
      *   A DoricColumn referece of the provided type T
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
      * @group All Types
      * @param colName
      *   the alias to set the column.
      * @return
      *   DoricColumn with the alias
      */
    def as(colName: CName): NamedDoricColumn[T] =
      NamedDoricColumn[T](column, colName)

    /**
      * Type safe equals between Columns
      * @group All Types
      * @param other
      *   the column to compare
      * @return
      *   a reference to a Boolean DoricColumn with the comparation
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
      */
    def isIn(elems: T*): BooleanColumn = column.elem.map(_.isin(elems: _*)).toDC

    /**
      * Checks if the value of the column is null
      * @group All Types
      * @return
      *   Boolean DoricColumn
      */
    def isNull: BooleanColumn = column.elem.map(_.isNull).toDC

    /**
      * Checks if the value of the column is not null
      * @group All Types
      * @return
      *   Boolean DoricColumn
      */
    def isNotNull: BooleanColumn = column.elem.map(_.isNotNull).toDC

    /**
      * Checks if the value of the column is not a number
      * @group All Types
      * @return
      *   Boolean DoricColumn
      */
    def isNaN: BooleanColumn = column.elem.map(_.isNaN).toDC

  }

}
