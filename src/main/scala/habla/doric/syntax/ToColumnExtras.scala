package habla.doric
package syntax

import org.apache.spark.sql.Column

trait ToColumnExtras {

  implicit class ToColumnSyntax[T: FromDf](column: T) {
    def sparkColumn: Column = implicitly[FromDf[T]].column(column)
  }

}
