package doric

import org.scalactic._
import TripleEquals._
import Tolerance._
import java.sql.Date
import java.time.{Instant, LocalDate}

object Equalities {

  implicit def eqOptional[O: Equality]: Equality[Option[O]] = {
    case (Some(x: Double), Some(y: Double)) => x === y +- 0.00001
    case (Some(x), Some(y))                 => x === y
    case (None, None)                       => true
    case _                                  => false
  }

  implicit def eqList[O: Equality]: Equality[List[O]] = {
    case (a: List[O], b: List[Any]) =>
      a.zip(b).forall(x => x._1 === x._2)
    case _ => false
  }

  implicit val eqInstant: Equality[Instant] = {
    case (a: Instant, b: Instant) => a.compareTo(a) === 0
    case _                        => false
  }

  implicit val eqDate: Equality[Date] = {
    case (a: Date, b: Date) => a.compareTo(b) === 0
    case _                  => false
  }

  implicit val eqLocalDate: Equality[LocalDate] = {
    case (a: LocalDate, b: LocalDate) => a.isEqual(b)
    case _                            => false
  }
}
