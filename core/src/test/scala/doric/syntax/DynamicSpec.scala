package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row

class DynamicSpec extends DoricTestElements with EitherValues with Matchers {

  import spark.implicits._

  private val df = List((User("John", "doe", 34), 1))
    .toDF("user", "delete")

  describe("Dynamic invocations") {

    it("can get values from sub-columns of `Row`` columns") {
      colStruct("user").child.name[String]
      df.validateColumnType(colStruct("user").child.name[String])
      df.validateColumnType(colStruct("user").child.age[Int])
    }

    it("can get values from sub-sub-columns") {
      List(((("1", 2.0), 2), true))
        .toDF()
        .validateColumnType(colStruct("_1").child._1[Row].child._1[String])
    }

    it("can get values from the top-level row") {
      df.validateColumnType(row.user[Row])
      df.validateColumnType(row.user[Row].child.age[Int])
      List(("1", 2, true)).toDF().validateColumnType(row._1[String])
      List((("1", 2), true))
        .toDF()
        .validateColumnType(row._1[Row].child._2[Int])
    }

    if (minorScalaVersion >= 12)
      it("should not compile if the parent column is not a row") {
        """val c: DoricColumn[String] = col[Int]("id").child.name[String]""" shouldNot compile
      }
  }
}
