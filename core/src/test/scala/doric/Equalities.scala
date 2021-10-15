package doric

import org.scalactic._
import TripleEquals._

import java.sql.Date
import java.time.{Instant, LocalDate}

object Equalities {

  implicit def eqOptional[O: Equality]: Equality[Option[O]] =
    (a: Option[O], b: Any) =>
      (a, b) match {
        case (Some(x), Some(y)) => x === y
        case (None, None)       => true
        case _                  => false
      }

  implicit def eqList[O: Equality]: Equality[List[O]] =
    (a: List[O], b: Any) =>
      b match {
        case l :: rest =>
          if (a.size == rest.size + 1) {
            val zipped = a.zip(l :: rest)
            val bools  = zipped.map(x => x._1 === x._2)
            val r      = bools.reduce(_ && _)
            r
          } else false
        case _ => a === b
      }

  implicit val eqInstant: Equality[Instant] = (a: Instant, b: Any) =>
    b match {
      case x: Instant => x.compareTo(a) === 0
      case _          => a === b
    }

  implicit val eqDate: Equality[Date] = (a: Date, b: Any) =>
    b match {
      case x: Date => x.compareTo(a) === 0
      case _       => a === b
    }

  implicit val eqLocalDate: Equality[LocalDate] = (a: LocalDate, b: Any) =>
    b match {
      case x: LocalDate => x.isEqual(a)
      case _            => a === b
    }
}
