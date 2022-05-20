package doric
package syntax

import doric.matchers.CustomMatchers.includeErrors
import doric.sem.{ChildColumnNotFound, ColumnTypeError, DoricMultiError}
import doric.types.SparkType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

case class User(name: String, surname: String, age: Int)

class DStructOpsSpec extends DoricTestElements with EitherValues with Matchers {

  import spark.implicits._

  private val df = List((User("John", "doe", 34), 1))
    .toDF("col", "delete")
    .select("col")

  describe("Dynamic struct column") {
    it("can get values subcolumns") {
      df.validateColumnType(colStruct("col").getChild[String]("name"))
      df.validateColumnType(colStruct("col").getChild[Int]("age"))
    }

    it("creates a struct from different columns") {
      val dfd = List((1, "hi"), (2, "bye"))
        .toDF("num", "str")
        .select(struct(colInt("num"), colString("str")) as "stru")

      dfd.validateColumnType(colStruct("stru").getChild[String]("str"))
      dfd.validateColumnType(colStruct("stru").getChild[Int]("num"))
    }

    it("generates a error if the sub column doesn't exist") {
      colStruct("col")
        .getChild[String]("jander")
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head shouldBe ChildColumnNotFound(
        "jander",
        List("name", "surname", "age")
      )
    }

    it("throws an error if the sub column is not of the provided type") {
      intercept[DoricMultiError] {
        df.select(
          //colStruct("col").getChild[Int]("name"),
          colStruct("col").getChild[String]("age")
        )
      } should includeErrors(
        //ColumnTypeError("name", IntegerType, StringType),
        ColumnTypeError("age", StringType, IntegerType)
      )
    }

    it(
      "throws an error if the user forces a field access for non-row columns"
    ) {
      colInt("delete")
        .asInstanceOf[RowColumn]
        .getChild[Int]("name")
        .elem
        .run(
          List((User("John", "doe", 34), 1))
            .toDF("col", "delete")
        )
        .toEither
        .left
        .value
        .head shouldBe ColumnTypeError(
        "delete",
        SparkType[Row].dataType,
        IntegerType
      )
    }
  }

}
