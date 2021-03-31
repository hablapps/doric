package habla.doric
package syntax

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

trait FromDfExtras {

  @inline def dataType[T: FromDf]: DataType = FromDf[T].dataType

}
