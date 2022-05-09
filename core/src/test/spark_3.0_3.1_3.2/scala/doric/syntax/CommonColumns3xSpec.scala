package doric
package syntax

import doric.DoricTestElements
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

import org.apache.spark.sql.{functions => f}

class CommonColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("xxhash64 doric function") {
    import spark.implicits._

    it("should work as spark xxhash64 function") {
      val df = List(
        ("this is a string", "123"),
        (null, "123"),
        ("123", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => xxhash64(colString(col1), colString(col2)),
        (col1, col2) => f.xxhash64(f.col(col1), f.col(col2)),
        List(
          Some(-6297204973024389939L),
          Some(3994740064877260556L),
          Some(3994740064877260556L),
          Some(42L)
        )
      )
    }

    it("should work with multiple types") {
      val df = List(
        ("whatever", 123, 12L, 4.0, true),
        (null, -4, -4572L, 0.0, false)
      ).toDF("string", "int", "long", "double", "boolean")

      df.testColumnsN(df.schema)(
        seq => xxhash64(seq: _*),
        seq => f.xxhash64(seq: _*),
        List(Some(-7858579933223513963L), Some(-1356518162039135835L))
      )
    }
  }
}
