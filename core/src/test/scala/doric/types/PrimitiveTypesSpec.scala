package doric
package types

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

// Note: checked out from https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L745

class PrimitiveTypesSpec extends DoricTestElements {

  describe("Simple Java/Scala types") {

    it("should match Atomic Spark SQL types") {

      // Null type

      testDataType[Null]
      testLitDataType[Null](null)

      // Numeric types

      testDataType[Int]
      testDataType[Long]
      testDataType[Float]
      testDataType[Double]
      testDataType[Short]
      testDataType[Byte]

      testLitDataType[Int](0)
      testLitDataType[Long](0L)
      testLitDataType[Float](0.0F)
      testLitDataType[Double](0.0)
      testLitDataType[Short](0)
      testLitDataType[Byte](0)

      testDataType[java.lang.Integer]
      testDataType[java.lang.Long]
      testDataType[java.lang.Double]
      testDataType[java.lang.Float]
      testDataType[java.lang.Short]
      testDataType[java.lang.Byte]

      /*
      testLitDataType[java.lang.Integer](0)
      testLitDataType[java.lang.Long](0L)
      testLitDataType[java.lang.Double](0.0)
      testLitDataType[java.lang.Float](0.0F)
      testLitDataType[java.lang.Short](java.lang.Short.MAX_VALUE)
      testLitDataType[java.lang.Byte](java.lang.Byte.MAX_VALUE)
      */

      testDataType[BigDecimal]
      testDataType[java.math.BigDecimal]
      testDataType[java.math.BigInteger]
      testDataType[scala.math.BigInt]
      testDataType[Decimal]

      /*
      testLitDataType[BigDecimal](BigDecimal(0.0))
      testLitDataType[java.math.BigDecimal](java.math.BigDecimal.ZERO)
      testLitDataType[java.math.BigInteger](java.math.BigInteger.ZERO)
      //testLitDataType[scala.math.BigInt](0)
      testLitDataType[Decimal](Decimal(0))
      */

      // String types

      testDataType[String]

      testLitDataType[String]("")

      //*VarcharType
      //*CharType

      // Binary type

      testDataType[Array[Byte]]
      testLitDataType[Array[Byte]](Array(0,0))

      // Boolean type

      testDataType[Boolean]
      testDataType[java.lang.Boolean]

      testLitDataType[Boolean](true)
      //testLitDataType[java.lang.Boolean](true)

      // Datetime type

      testDataType[java.sql.Date]
      testDataType[java.sql.Timestamp]
      testDataType[java.time.LocalDate]
      testDataType[java.time.Instant]
      testDataType[CalendarInterval]

      testLitDataType[java.sql.Date](java.sql.Date.valueOf("2022-12-31"))
      testLitDataType[java.sql.Timestamp](new java.sql.Timestamp(0))
      testLitDataType[java.time.LocalDate](java.time.LocalDate.now())
      testLitDataType[java.time.Instant](java.time.Instant.now())
      //testLitDataType[CalendarInterval](new CalendarInterval(0, 0, 0))


      // Interval type

      testDataType[java.time.Duration]
      testDataType[java.time.Period]

      /*
      testLitDataType[java.time.Duration](java.time.Duration.ZERO)
      testLitDataType[java.time.Period](java.time.Period.ZERO)
      */
    }
  }

  describe("Collection types"){

    it("should match Spark Array types") {
      testDataType[Array[Int]]
      testDataType[Seq[Int]]
      testDataType[List[Int]]
      testDataType[IndexedSeq[Int]]
      testDataType[Set[Int]]

      testDataType[Array[String]]
      testDataType[Seq[String]]
      testDataType[List[String]]
      testDataType[IndexedSeq[String]]
      testDataType[Set[String]]

      testLitDataType[Array[Int]](Array(0,0))
      testLitDataType[Seq[Int]](Seq(0,0))
      testLitDataType[List[Int]](List(0,0))
      testLitDataType[IndexedSeq[Int]](IndexedSeq(0,0))
      //testLitDataType[Set[Int]](Set(0,1))

      testLitDataType[Array[String]](Array("",""))
      testLitDataType[Seq[String]](Seq("", ""))
      testLitDataType[List[String]](List("", ""))
      testLitDataType[IndexedSeq[String]](IndexedSeq("", ""))
      //testLitDataType[Set[String]](Set("", "a"))


    }

    it("should match Spark Map types") {

      testDataType[Map[Int, String]]
      testDataType[Map[String, Int]]
      testDataType[Map[Int, Int]]
      testDataType[Map[String, String]]

      testLitDataType[Map[Int, String]](Map(0->""))
      testLitDataType[Map[String, Int]](Map(""->0))
      testLitDataType[Map[Int, Int]](Map(0->0))
      testLitDataType[Map[String, String]](Map(""->""))

    }

    it("should match Spark Option types") {

      testDataType[Option[Int]]

      testLitDataType[Option[Int]](Some(0))
      testLitDataType[Option[Int]](None)
    }
  }

  case class User(name: String, age: Int)

  describe("Struct types"){
    import org.apache.spark.sql.{functions => f}
    spark.range(1).select(f.typedlit[Map[Int, String]](Map[Int,String](1->"", 2->""))).show
    ignore("should match `Row`"){
      testDataType[Row]
    }

    it("should match case classes") {

      testDataType[(Int, String)]
      testDataType[User]
      //testLitDataType[(Int, String)]((0,""))
      //testLitDataType[User](User("", 0))

    }
  }
/*
  describe("Complex types"){
    it("should match a combination of Spark types"){
      testDataType[List[User]]
      testDataType[Array[List[Int]]]
      testDataType[Map[Int, Option[List[User]]]]
      testDataType[(List[Int], User, Map[Int, Option[User]])]

      testLitDataType[Array[User]](Array(User("", 0)))
      testLitDataType[List[User]](List(User("", 0)))
      testLitDataType[(User, Int)]((User("", 0), 1))
      testLitDataType[Array[Array[Int]]](Array(Array(1)))
      testLitDataType[Array[Array[User]]](Array(Array(User("",0), User("",0)), Array()))
      testLitDataType[Array[List[User]]](Array(List(User("",0), User("",0)), List()))
      testLitDataType[Map[Int, Option[List[User]]]](Map(0->None, 1->Some(List(User("",0)))))
      //testLitDataType[(List[Int], User, Map[Int, Option[User]])]((List(0,0), User("",0), Map(0->None, 1->Some(User("",0)))))
    }
  }
*/

  // TBD

  // Enumeration#Value / java.lang.Enum[_] => StringType
  // SQLUserDefinedType
  // UDTRegistration
}


