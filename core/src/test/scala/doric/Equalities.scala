package doric

import java.sql.Date
import java.time.{Instant, LocalDate}
import org.scalactic._
import org.scalactic.Tolerance._
import org.scalactic.TripleEquals._

import scala.math.BigDecimal.javaBigDecimal2bigDecimal

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

  implicit val eqBigDecimal: Equality[BigDecimal] = new Equality[BigDecimal] {
    override def areEqual(a: BigDecimal, b: Any): Boolean = (a, b) match {
      case (x: BigDecimal, y: BigDecimal) => x === y +- 0.00001
      case _                              => false
    }
  }

  implicit val eqJavaBigDecimal: Equality[java.math.BigDecimal] =
    new Equality[java.math.BigDecimal] {
      override def areEqual(a: java.math.BigDecimal, b: Any): Boolean =
        (a, b) match {
          case (x: java.math.BigDecimal, y: java.math.BigDecimal) =>
            x >= (y - 0.00001) && x <= (y + 0.00001)
          case _ => false
        }
    }

  implicit val eqSparkDecimal: Equality[org.apache.spark.sql.types.Decimal] =
    new Equality[org.apache.spark.sql.types.Decimal] {
      override def areEqual(
          a: org.apache.spark.sql.types.Decimal,
          b: Any
      ): Boolean = (a, b) match {
        case (
              x: org.apache.spark.sql.types.Decimal,
              y: org.apache.spark.sql.types.Decimal
            ) =>
          x.equals(y)
        case (x, y) =>
          false
      }
    }

  implicit val eqJavaBigInteger: Equality[java.math.BigInteger] =
    new Equality[java.math.BigInteger] {
      override def areEqual(a: java.math.BigInteger, b: Any): Boolean =
        (a, b) match {
          case (x: java.math.BigInteger, y: java.math.BigInteger) =>
            x.compareTo(y) == 0
          case (x, y) =>
            false
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
