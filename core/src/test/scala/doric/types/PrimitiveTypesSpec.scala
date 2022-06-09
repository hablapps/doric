package doric
package types

import doric.types.SparkType.Primitive
import doric.types.customTypes.User
import doric.types.customTypes.User.{userlst, userst}
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.scalactic.source

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime}

class PrimitiveTypesSpec extends DoricTestElements {

  import spark.implicits._

  describe("Simple Java/Scala types") {

    it("should match Atomic Spark SQL types") {
      // Note: checked out from https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L745

      // Null type

      testDataType[Null]

      // Numeric types

      testDataType[Int]
      testDataType[Long]
      testDataType[Float]
      testDataType[Double]
      testDataType[Short]
      testDataType[Byte]

      testDataType[java.lang.Integer]
      testDataType[java.lang.Long]
      testDataType[java.lang.Double]
      testDataType[java.lang.Float]
      testDataType[java.lang.Short]
      testDataType[java.lang.Byte]

      testDataType[BigDecimal]
      testDataType[java.math.BigDecimal]
      testDataType[java.math.BigInteger]
      testDataType[scala.math.BigInt]
      testDataType[Decimal]

      // String types

      testDataType[String]
      //*VarcharType
      //*CharType

      // Binary type

      testDataType[Array[Byte]]

      // Boolean type

      testDataType[Boolean]
      testDataType[java.lang.Boolean]

      // Datetime type

      testDataType[java.sql.Date]
      testDataType[java.sql.Timestamp]
      testDataType[java.time.LocalDate]
      testDataType[java.time.Instant]
      testDataType[CalendarInterval]

      // Interval type

      testDataType[java.time.Duration]
      testDataType[java.time.Period]
    }
  }

  describe("Collection types"){

    it("should match Spark Array types") {

      testDataType[Array[Int]]
      testDataType[Seq[Int]]
      testDataType[List[Int]]
      testDataType[IndexedSeq[Int]]
      testDataType[Set[Int]]
    }

    it("should match Spark Map types") {

      testDataType[Map[Int, String]]
    }

    it("should match Spark Option types") {

      testDataType[Option[Int]]
    }
  }

  case class User(name: String, age: Int)

  describe("Product types"){

    it("should match StructTypes") {

      testDataType[(Int, String)]
      testDataType[User]
      testDataType[(List[Int], User, Map[Int, Option[User]])]
    }
  }


  // TBD

  // Enumeration#Value / java.lang.Enum[_] => StringType
  // SQLUserDefinedType
  // UDTRegistration
}


