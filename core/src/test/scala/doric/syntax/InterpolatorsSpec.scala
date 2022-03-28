package doric
package syntax

import org.apache.spark.sql.{functions => f}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

class InterpolatorsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("String interpolator") {
    import spark.implicits._

    val colName = "column1"
    val df = List("value 1", "value 2")
      .toDF(colName)

    it("should be a String Column") {
      df.validateColumnType[String](ds"column")
      df.validateColumnType[String](ds"column ${colString(colName)}")
      df.validateColumnType[String](ds"${colString(colName)}")
    }

    it("should concat literals and columns") {
      df.testColumns2(colName, "Column has value:")(
        (c, str) => ds"${str.lit} -->${colString(c)}",
        (c, str) => f.concat(f.lit(str), f.lit(" -->"), f.col(c)),
        List(
          Some("Column has value: -->value 1"),
          Some("Column has value: -->value 2")
        )
      )
    }

    it("should work if null columns") {
      val dfNull = List("value 1", null)
        .toDF(colName)
      dfNull.testColumns2(colName, "Column has value:")(
        (c, str) => ds"${str.lit} -->${colString(c)}",
        (c, str) =>
          f.concat(f.lit(str), f.lit(" -->"), f.coalesce(f.col(c), f.lit(""))),
        List(
          Some("Column has value: -->value 1"),
          Some("Column has value: -->")
        )
      )
    }
  }

}
