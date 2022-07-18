package doric
package types

import doric.Equalities._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

class SerializeSparkTypeSpec
    extends DoricTestElements
    with SerializeSparkTypeSpec_Specific {

  describe("Simple Java/Scala types") {

    it("should match Atomic Spark SQL types") {

      // Null type

      serializeSparkType[Null]((null))

      // Numeric types

      serializeSparkType[Int](0)
      serializeSparkType[Long](0L)
      serializeSparkType[Float](0.0f)
      serializeSparkType[Double](0.0)
      serializeSparkType[Short](0)
      serializeSparkType[Byte](0)

      /*
      serializeSparkType[java.lang.Integer](0)
      serializeSparkType[java.lang.Long](0L)
      serializeSparkType[java.lang.Double](0.0)
      serializeSparkType[java.lang.Float](0.0F)
      serializeSparkType[java.lang.Short](java.lang.Short.MAX_VALUE)
      serializeSparkType[java.lang.Byte](java.lang.Byte.MAX_VALUE)

      serializeSparkType[BigDecimal](BigDecimal(0.0))
      serializeSparkType[java.math.BigDecimal](java.math.BigDecimal.ZERO)
      serializeSparkType[java.math.BigInteger](java.math.BigInteger.ZERO)
      //serializeSparkType[scala.math.BigInt](0)
      serializeSparkType[Decimal](Decimal(0))
       */

      // String types

      serializeSparkType[String]("")
      // VarcharType
      // CharType

      // Binary type

      serializeSparkType[Array[Byte]](Array(0, 0))

      // Boolean type

      serializeSparkType[Boolean](true)
      serializeSparkType[java.lang.Boolean](true)

      // Datetime type

      SQLConf.withExistingConf(
        spark.sessionState.conf.copy(DoricTestElements.JAVA8APIENABLED -> false)
      ) {
        serializeSparkType[java.sql.Date](java.sql.Date.valueOf("2022-12-31"))
        serializeSparkType[java.sql.Timestamp](new java.sql.Timestamp(0))
      }

      if (spark.version > "2.4.8")
        SQLConf.withExistingConf(
          spark.sessionState.conf
            .copy(DoricTestElements.JAVA8APIENABLED -> true)
        ) {
          serializeSparkType[java.time.LocalDate](java.time.LocalDate.now())
          serializeSparkType[java.time.Instant](java.time.Instant.now())
        }

      // TBD: CalendarIntervalType
    }
  }

  describe("Collection types") {

    it("should match Spark Array types") {

      serializeSparkType[Array[Int]](Array(0, 0))
      serializeSparkType[Seq[Int]](Seq(0, 0))
      serializeSparkType[List[Int]](List(0, 0))
      serializeSparkType[IndexedSeq[Int]](IndexedSeq(0, 0))
      serializeSparkType[Set[Int]](Set(0, 1))

      serializeSparkType[Array[String]](Array("", ""))
      serializeSparkType[Seq[String]](Seq("", ""))
      serializeSparkType[List[String]](List("", ""))
      serializeSparkType[IndexedSeq[String]](IndexedSeq("", ""))
      serializeSparkType[Set[String]](Set("", "a"))

    }

    it("should match Spark Map types") {

      serializeSparkType[Map[Int, String]](Map(0 -> ""))
      serializeSparkType[Map[String, Int]](Map("" -> 0))
      serializeSparkType[Map[Int, Int]](Map(0 -> 0))
      serializeSparkType[Map[String, String]](Map("" -> ""))
    }

    it("should match Spark Option types") {

      serializeSparkType[Option[Int]](Some(0))
      serializeSparkType[Option[Int]](None)
    }
  }

  case class User(name: String, age: Int)

  describe("Struct types") {

    it("should match case classes") {

      serializeSparkType[(Int, String)]((0, ""))
      serializeSparkType[User](User("", 0))
    }
  }

  describe("Complex types") {
    it("should match a combination of Spark types") {
      serializeSparkType[Array[User]](Array(User("", 0)))
      serializeSparkType[List[User]](List(User("", 0)))
      serializeSparkType[(User, Int)]((User("", 0), 1))
      serializeSparkType[Array[Array[Int]]](Array(Array(1)))
      serializeSparkType[Array[Array[User]]](
        Array(Array(User("", 0), User("", 0)), Array())
      )
      serializeSparkType[Array[List[User]]](
        Array(List(User("", 0), User("", 0)), List())
      )
      serializeSparkType[Map[Int, Option[List[User]]]](
        Map(0 -> None, 1 -> Some(List(User("", 0))))
      )
      serializeSparkType[(List[Int], User, Map[Int, Option[User]])](
        (List(0, 0), User("", 0), Map(0 -> None, 1 -> Some(User("", 0))))
      )
    }
  }

}
