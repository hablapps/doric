package doric
package syntax

import java.sql.Timestamp
import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.{functions => f}

class Numeric31Spec
    extends SparkSessionTestWrapper
    with AnyFunSpecLike
    with TypedColumnTest {

  describe("timestampSeconds doric function") {
    import spark.implicits._

    it("should work as spark timestamp_seconds function with integers") {
      val df = List(Some(123), Some(1), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colInt(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01")),
          None
        )
      )
    }

    it("should work as spark timestamp_seconds function with longs") {
      val df = List(Some(123L), Some(1L), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colLong(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01")),
          None
        )
      )
    }

    it("should work as spark timestamp_seconds function with doubles") {
      val df = List(Some(123.2), Some(1.9), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colDouble(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03.2")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01.9")),
          None
        )
      )
    }
  }

}
