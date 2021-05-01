package habla.doric
package types

import habla.doric.syntax.DateColumnLike
import java.sql.Date
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

trait DoricDateType {

  type DateColumn = DoricColumn[Date]

  object DateColumn {

    def unapply(column: Column): Option[DateColumn] =
      DoricColumnExtr.unapply[Date](column)

  }

  implicit val fromDate: FromDf[Date] = new FromDf[Date] {
    override val dataType: DataType = DateType
  }

  implicit val fromLocalDate: FromDf[LocalDate] = new FromDf[LocalDate] {
    override val dataType: DataType = DateType
  }

  implicit val dateCol: DateColumnLike[Date] = new DateColumnLike[Date] {}

  implicit val localdateOps: DateColumnLike[Instant] =
    new DateColumnLike[Instant] {}

}
