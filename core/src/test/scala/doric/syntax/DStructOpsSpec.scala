package doric
package syntax

import doric.sem.{ChildColumnNotFound, ColumnTypeError, DoricMultiError}
import doric.types.SparkType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType}

case class User(name: String, surname: String, age: Int)

class DStructOpsSpec extends DoricTestElements {

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
      intercept[DoricMultiError] {
        df.select(
          colStruct("col").getChild[String]("jander")
        )
      } should containAllErrors(
        ChildColumnNotFound("jander", List("name", "surname", "age"))
      )
    }

    it("throws an error if the sub column is not of the provided type") {
      intercept[DoricMultiError] {
        df.select(
          colStruct("col").getChild[String]("age")
        )
      } should containAllErrors(
        ColumnTypeError("age", StringType, IntegerType)
      )
    }

    it(
      "throws an error if the user forces a field access for non-row columns"
    ) {
      intercept[DoricMultiError] {
        List((User("John", "doe", 34), 1))
          .toDF("col", "delete")
          .select(
            colInt("delete").asInstanceOf[RowColumn].getChild[Int]("name")
          )
      } should containAllErrors(
        ColumnTypeError("", SparkType[Row].dataType, IntegerType)
      )
    }
  }

  val dfUsers =
    List((User("name1", "", 1), 1), (User("name2", "", 2), 2), (User("name3", "", 3), 3))
      .toDF("user", "delete")

  describe("Field access") {

    it("should work for product types, dynamically") {
      dfUsers
        .select(col[User]("user").getChild[Int]("age") as "age")
        .collectCols(col[Int]("age")) shouldBe
        List(1, 2, 3)

      dfUsers
        .select(col[User]("user").getChild[String]("name") as "name")
        .collectCols(col[String]("name")) shouldBe
        List("name1", "name2", "name3")
    }

    it("should work statically as well") {
      dfUsers
        .select(col[User]("user").getChildSafe('name) as "name")
        .collectCols(col[String]("name")) shouldBe
        List("name1", "name2", "name3")
    }

    it("should not work statically if the field doesn't exist") {
      """col[User]("user").getChildSafe('nameeee)""" shouldNot compile
    }
  }

}
