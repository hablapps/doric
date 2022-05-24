package doric.types

import doric.DoricTestElements
import doric.types.customTypes.User
import doric.types.customTypes.User.{userlst, userst}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.expressions.Literal

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime}

class PrimitiveTypesSpec extends DoricTestElements {

  describe("Primitive types"){
    it("should have expected Spark data types"){
      testDataTypeFor(null)
      testDataTypeFor(Option(1))
      // UserDefinedType?
      testDataTypeFor(Array[Byte]())
      testDataTypeFor(Array[Int](1,2,3))
      // testDataTypeFor(Array[Option[Int]](Some(1), None)) // wrong nullability
      testDataTypeFor(Seq(1,2,3))
      testDataTypeFor(List(1,2,3))
      testDataTypeFor(IndexedSeq(1,2,3))
      testDataTypeFor(Map[Int, String]())
      testDataTypeFor(Set[Int]())
      testDataTypeFor("")
      testDataTypeFor( Instant.EPOCH)
      testDataTypeFor(new Timestamp(0))
      //testDataTypeFor(LocalDateTime.MAX)
      testDataTypeFor(LocalDate.MAX)
      testDataTypeFor(new java.sql.Date(0))
      /*
      Schema(CalendarIntervalType, nullable = true) shouldBe theDataTypeFor(// CalendarInterval)
      testDataTypeFor(new java.time.Duration)
      testDataTypeFor(new java.time.Period)
      Schema(DecimalType.SYSTEM_DEFAULT, nullable = true) shouldBe theDataTypeFor(// BigDecimal)
      testDataTypeFor(new java.math.BigDecimal)
      testDataTypeFor(new java.math.BigInteger)
      Schema(DecimalType.BigIntDecimal, nullable = true) shouldBe theDataTypeFor(// scala.math.BigInt)
      Schema(DecimalType.SYSTEM_DEFAULT, nullable = true) shouldBe theDataTypeFor(// Decimal)
      */
      /*
      testDataTypeFor[java.lang.Integer](0)
      testDataTypeFor[java.lang.Long](0)
      testDataTypeFor[java.lang.Double](0.0)
      testDataTypeFor[java.lang.Float](0.0)
      testDataTypeFor[java.lang.Short](0)
      testDataTypeFor[java.lang.Byte](0)
      testDataTypeFor[java.lang.Boolean](true)
      testDataTypeFor[java.lang.Enum[_]]()

       */
      testDataTypeFor[Int](0)
      testDataTypeFor[Long](0)
      testDataTypeFor[Double](0.0)
      testDataTypeFor[Float](0.0F)
      testDataTypeFor[Short](0)
      testDataTypeFor[Byte](0)
      testDataTypeFor(true)
      // Enumeration#Value
    }
  }


}
