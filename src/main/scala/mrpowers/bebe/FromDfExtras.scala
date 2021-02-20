package mrpowers.bebe

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column

trait FromDfExtras {

  def construct[T: FromDf](column: Column): T = implicitly[FromDf[T]].construct(column)

  def dataType[T: FromDf]: DataType = implicitly[FromDf[T]].dataType

}
