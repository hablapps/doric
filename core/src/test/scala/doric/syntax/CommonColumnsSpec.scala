package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class CommonColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  import doric.implicitConversions.stringCname

  describe("coalesce doric function") {
    import spark.implicits._

    it("should work as spark coalesce function with strings") {
      val df = List(("1", "1"), (null, "2"), ("3", null), (null, null))
        .toDF("col1", "col2")
      df.testColumns2("col1", "col2")(
        (col1, col2) => coalesce(colString(col1), colString(col2)),
        (col1, col2) => f.coalesce(f.col(col1), f.col(col2)),
        List("1", "2", "3", null).map(Option(_))
      )
    }

    it("should work as spark coalesce function with integers") {
      val df =
        List((Some(1), Some(1)), (None, Some(2)), (Some(3), null), (null, null))
          .toDF("col1", "col2")
      df.testColumns2("col1", "col2")(
        (col1, col2) => coalesce(colInt(col1), colInt(col2)),
        (col1, col2) => f.coalesce(f.col(col1), f.col(col2)),
        List(Some(1), Some(2), Some(3), None)
      )
    }
  }

}
