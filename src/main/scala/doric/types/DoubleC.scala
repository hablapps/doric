package doric.types

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

trait DoubleC[T]

object DoubleC {
  implicit val intNumeric: DoubleC[Int]             = new DoubleC[Int] {}
  implicit val longNumeric: DoubleC[Long]           = new DoubleC[Long] {}
  implicit val floatNumeric: DoubleC[Float]         = new DoubleC[Float] {}
  implicit val doubleNumeric: DoubleC[Double]       = new DoubleC[Double] {}
  implicit val dateCol: DoubleC[Date]               = new DoubleC[Date] {}
  implicit val localdateOps: DoubleC[LocalDate]     = new DoubleC[LocalDate] {}
  implicit val timestampDateOps: DoubleC[Timestamp] = new DoubleC[Timestamp] {}
  implicit val instantDateOps: DoubleC[Instant]     = new DoubleC[Instant] {}
}
