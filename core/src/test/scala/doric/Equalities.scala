package doric

import java.sql.Date
import java.time.{Instant, LocalDate}
import org.scalactic._
import org.scalactic.Tolerance._
import org.scalactic.TripleEquals._

object Equalities {

  implicit def eqOptional[O: Equality]: Equality[Option[O]] =
    new Equality[Option[O]] {
      override def areEqual(a: Option[O], b: Any): Boolean = (a, b) match {
        case (Some(x), Some(y)) => x === y
        case (None, None)       => true
        case _                  => false
      }
    }

  implicit def eqList[O: Equality]: Equality[List[O]] = new Equality[List[O]] {
    override def areEqual(a: List[O], b: Any): Boolean = (a, b) match {
      case (a: List[O], b: List[Any]) =>
        a.zip(b).forall(x => x._1 === x._2)
      case _ => false
    }
  }

  implicit val eqDouble: Equality[Double] = new Equality[Double] {
    override def areEqual(a: Double, b: Any): Boolean = (a, b) match {
      case (x: Double, y: Double) => x === y +- 0.00001
      case _                      => false
    }
  }

  implicit val eqInstant: Equality[Instant] = new Equality[Instant] {
    override def areEqual(a: Instant, b: Any): Boolean = (a, b) match {
      case (a: Instant, b: Instant) => a.compareTo(b) === 0
      case _                        => false
    }
  }

  implicit val eqDate: Equality[Date] = new Equality[Date] {
    override def areEqual(a: Date, b: Any): Boolean = (a, b) match {
      case (a: Date, b: Date) => a.compareTo(b) === 0
      case _                  => false
    }
  }

  implicit val eqLocalDate: Equality[LocalDate] = new Equality[LocalDate] {
    override def areEqual(a: LocalDate, b: Any): Boolean = (a, b) match {
      case (a: LocalDate, b: LocalDate) => a.isEqual(b)
      case _                            => false
    }
  }
}
