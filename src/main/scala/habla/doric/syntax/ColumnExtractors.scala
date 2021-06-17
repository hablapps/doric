package habla.doric
package syntax

import habla.doric.types.SparkType

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

trait ColumnExtractors extends ColGetters[DoricColumn] {

  @inline def dataType[T: SparkType]: DataType = SparkType[T].dataType

  override protected def constructSide[T](
      column: Doric[Column]
  ): DoricColumn[T] =
    DoricColumn(column)

}
