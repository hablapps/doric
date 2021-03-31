package habla.doric
package syntax

import org.apache.spark.sql.Column

trait ToColumnExtras {

  implicit class ToColumnSyntax[T: FromDf](column: DoricColumn[T]) {
    def sparkColumn: Column = FromDf[T].column(column)
  }

}
