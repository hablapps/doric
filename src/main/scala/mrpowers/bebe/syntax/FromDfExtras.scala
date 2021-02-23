package mrpowers.bebe
package syntax

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  def construct[T: FromDf](column: Column): T = implicitly[FromDf[T]].construct(column)

  def dataType[T: FromDf]: DataType = implicitly[FromDf[T]].dataType

}
