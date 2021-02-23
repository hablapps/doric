package mrpowers.bebe
package syntax

import org.apache.spark.sql.Column

trait ToColumnExtras {

  implicit class ToColumnSyntax[T: ToColumn](column: T) {
    def sparkColumn: Column = implicitly[ToColumn[T]].column(column)
  }

}
