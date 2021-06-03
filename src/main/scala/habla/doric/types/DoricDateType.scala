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

  implicit val fromDate: SparkType[Date] = new SparkType[Date] {
    override val dataType: DataType = DateType
  }

  implicit val fromLocalDate: SparkType[LocalDate] = new SparkType[LocalDate] {
    override val dataType: DataType = DateType
  }

  implicit val dateCol: DateColumnLike[Date] = new DateColumnLike[Date] {}

  implicit val localdateOps: DateColumnLike[Instant] =
    new DateColumnLike[Instant] {}

  implicit val dateCastToLocalDate: Casting[Date, LocalDate] =
    new SparkCasting[Date, LocalDate] {}

  implicit val localdateCastToLocalDate: Casting[LocalDate, Date] =
    new SparkCasting[LocalDate, Date] {}

}
