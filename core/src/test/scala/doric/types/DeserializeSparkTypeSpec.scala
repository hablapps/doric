package doric
package types

import org.apache.spark.unsafe.types.CalendarInterval
import Equalities._
import org.apache.spark.sql.types.Decimal

class DeserializeSparkTypeSpec extends DoricTestElements {

  describe("Simple Java/Scala types") {

    it("should match Atomic Spark SQL types") {

      // Null type

      deserializeSparkType[Null]((null))

      // Numeric types

      deserializeSparkType[Int](0)
      deserializeSparkType[Long](0L)
      deserializeSparkType[Float](0.0f)
      deserializeSparkType[Double](0.0)
      deserializeSparkType[Short](0)
      deserializeSparkType[Byte](0)

      deserializeSparkType[java.lang.Integer](0)
      deserializeSparkType[java.lang.Long](0L)
      deserializeSparkType[java.lang.Double](0.0)
      deserializeSparkType[java.lang.Float](0.0f)
      deserializeSparkType[java.lang.Short](java.lang.Short.MAX_VALUE)
      deserializeSparkType[java.lang.Byte](java.lang.Byte.MAX_VALUE)

      deserializeSparkType[BigDecimal](BigDecimal(0.0))
      deserializeSparkType[java.math.BigDecimal](java.math.BigDecimal.ZERO)
      deserializeSparkType[java.math.BigInteger](java.math.BigInteger.ZERO)
      // deserializeSparkType[scala.math.BigInt](0)
      deserializeSparkType[Decimal](Decimal(0))

      // String types

      deserializeSparkType[String]("")
      // VarcharType
      // CharType

      // Binary type

      deserializeSparkType[Array[Byte]](Array(0, 0))

      // Boolean type

      deserializeSparkType[Boolean](true)
      deserializeSparkType[java.lang.Boolean](true)

      // Datetime type

      deserializeSparkType[java.sql.Date](java.sql.Date.valueOf("2022-12-31"))
      deserializeSparkType[java.sql.Timestamp](new java.sql.Timestamp(0))
      deserializeSparkType[java.time.LocalDate](java.time.LocalDate.now())
      deserializeSparkType[java.time.Instant](java.time.Instant.now())
      deserializeSparkType[CalendarInterval](new CalendarInterval(0, 0, 0))

      // Interval type

      deserializeSparkType[java.time.Duration](java.time.Duration.ZERO)
      deserializeSparkType[java.time.Period](java.time.Period.ZERO)

    }
  }

  describe("Collection types") {

    it("should match Spark Array types") {

      deserializeSparkType[Array[Int]](Array(0, 0))
      deserializeSparkType[Seq[Int]](Seq(0, 0))
      deserializeSparkType[List[Int]](List(0, 0))
      deserializeSparkType[IndexedSeq[Int]](IndexedSeq(0, 0))
      // deserializeSparkType[Set[Int]](Set(0,1))

      deserializeSparkType[Array[String]](Array("", ""))
      deserializeSparkType[Seq[String]](Seq("", ""))
      deserializeSparkType[List[String]](List("", ""))
      deserializeSparkType[IndexedSeq[String]](IndexedSeq("", ""))
      // deserializeSparkType[Set[String]](Set("", "a"))

    }

    it("should match Spark Map types") {

      deserializeSparkType[Map[Int, String]](Map(0 -> ""))
      deserializeSparkType[Map[String, Int]](Map("" -> 0))
      deserializeSparkType[Map[Int, Int]](Map(0 -> 0))
      deserializeSparkType[Map[String, String]](Map("" -> ""))
    }

    it("should match Spark Option types") {

      deserializeSparkType[Option[Int]](Some(0))
      deserializeSparkType[Option[Int]](None)
    }
  }

  case class User(name: String, age: Int)

  describe("Struct types") {

    it("should match case classes") {

      deserializeSparkType[(Int, String)]((0, ""))
      deserializeSparkType[User](User("", 0))
    }
  }

  describe("Complex types") {
    it("should match a combination of Spark types") {
      deserializeSparkType[Array[User]](Array(User("", 0)))
      deserializeSparkType[List[User]](List(User("", 0)))
      deserializeSparkType[(User, Int)]((User("", 0), 1))
      deserializeSparkType[Array[Array[Int]]](Array(Array(1)))
      deserializeSparkType[Array[Array[User]]](
        Array(Array(User("", 0), User("", 0)), Array())
      )
      deserializeSparkType[Array[List[User]]](
        Array(List(User("", 0), User("", 0)), List())
      )
      deserializeSparkType[Map[Int, Option[List[User]]]](
        Map(0 -> None, 1 -> Some(List(User("", 0))))
      )
      deserializeSparkType[(List[Int], User, Map[Int, Option[User]])](
        (List(0, 0), User("", 0), Map(0 -> None, 1 -> Some(User("", 0))))
      )
    }
  }

}
