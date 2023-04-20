package doric

import doric.sem.{ColumnNotFound, DoricMultiError}
import doric.testUtilities.data.User
import doric.types.SparkType

import java.sql.{Date, Timestamp}
import org.scalatest.EitherValues
import org.apache.spark.sql.{Encoder, Row}

class DoricColumnSpec extends DoricTestElements with EitherValues {

  import spark.implicits._

  private val column = "column"

  def testValue[T: SparkType: Encoder](example: T): Unit = {
    val df = List(example).toDF(column)

    col[T](column).elem.run(df).toEither.value
  }
  def testValueNullable[T: SparkType](
      example: T
  )(implicit enc: Encoder[Option[T]]): Unit = {
    val df = List(Some(example), None).toDF(column)
    col[T](column).elem.run(df).toEither.value
  }

  describe("each column should represent their datatype") {
    it("works for String") {
      testValue[String]("hola")
      testValueNullable[String]("hola")
    }
    it("works for Int") {
      testValue[Int](14)
      testValueNullable[Int](54)
    }
    it("works for Long") {
      testValue[Long](14L)
      testValueNullable[Long](54L)
    }
    it("works for Float") {
      testValue[Float](14f)
      testValueNullable[Float](54f)
    }
    it("works for Array") {
      testValue[Array[Int]](Array(14))
      testValueNullable[Array[Int]](Array(54))
      testValue[Array[Long]](Array(14L))
      testValueNullable[Array[Long]](Array(54L))
      testValueNullable[Array[Array[Long]]](Array(Array(14L)))
    }
    it("works for Date") {
      testValue[Date](Date.valueOf("2020-01-01"))
      testValueNullable[Date](Date.valueOf("2020-01-01"))
    }
    val timestamp = Timestamp.valueOf("2020-01-01 01:01:901")
    it("works for Timestamp") {
      testValue[Timestamp](timestamp)
      testValueNullable[Timestamp](timestamp)
    }
    it("works for Map") {
      testValue[Map[Timestamp, Int]](Map(timestamp -> 10))
      testValueNullable[Map[Timestamp, Int]](Map(timestamp -> 10))
    }
    it("works for DStruct") {
      val df =
        List(((1, "hola"), 1)).toDF(column, "extra").select(column)
      col[Row](column).elem.run(df).toEither.value

      val df2 = List((Some((1, "hola")), 1), (None, 1))
        .toDF(column, "extra")
        .select(column)
      col[Row](column).elem.run(df2).toEither.value
    }
    it("works for structs if accessed directly") {
      val df = List((User("John", "doe", 34), 1))
        .toDF("col", "delete")
        .select("col")

      col[String]("col.name").elem.run(df).toEither.value
      col[Int]("col.age").elem.run(df).toEither.value
    }
    it("works for arrays if accessed directly an index") {
      val df = List((List("hola", "adios"), 1))
        .toDF(column, "delete")
        .select(column)

      (column.cname / c"0")[String].elem.run(df).toEither.value
      (column.cname / c"1")[String].elem.run(df).toEither.value
      (column.cname / c"2")[String].elem.run(df).toEither.value
    }
  }

  describe("DoricColumn") {
    val df = Seq("val1", "val2").toDF("myColumn")

    it("should create an uncheckedType") {
      val dCol: DoricColumn[_] = DoricColumn.uncheckedType("myColumn")

      dCol.elem.run(df).toEither shouldBe Right(df("myColumn"))
    }

    it("should fail creating an uncheckedType if not valid") {
      val dCol: DoricColumn[_] =
        DoricColumn.uncheckedType("nonExistentCol")

      intercept[DoricMultiError] {
        df.select(dCol)
      } should containAllErrors(
        ColumnNotFound("nonExistentCol", List("myColumn"))
      )
    }
  }

}
