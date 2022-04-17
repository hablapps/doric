package doric
package syntax

import doric.Equalities._
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class TimestampColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  val now: LocalDateTime = LocalDateTime.now

  describe("addMonths doric function with column") {
    import spark.implicits._

    val df = List(
      (Timestamp.valueOf(now), Some(1)),
      (Timestamp.valueOf(now), Some(-1)),
      (Timestamp.valueOf(now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("timestampCol", "monthCol")

    it("should work as spark add_months function with column") {
      df.testColumns2("timestampCol", "monthCol")(
        (d, m) => colTimestamp(d).addMonths(colInt(m)),
        (d, m) => f.add_months(f.col(d), f.col(m)),
        List(
          Date.valueOf(now.plusMonths(1).toLocalDate),
          Date.valueOf(now.minusMonths(1).toLocalDate),
          null,
          null,
          null
        ).map(Option(_))
      )
    }
  }

  describe("addDays doric function with column") {
    import spark.implicits._

    val df = List(
      (Timestamp.valueOf(now), Some(1)),
      (Timestamp.valueOf(now), Some(-1)),
      (Timestamp.valueOf(now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("timestampCol", "monthCol")

    it("should work as spark date_add function with column") {
      df.testColumns2("timestampCol", "monthCol")(
        (d, m) => colTimestamp(d).addDays(colInt(m)),
        (d, m) => f.date_add(f.col(d), f.col(m)),
        List(
          Date.valueOf(now.plusDays(1).toLocalDate),
          Date.valueOf(now.minusDays(1).toLocalDate),
          null,
          null,
          null
        ).map(Option(_))
      )
    }
  }

  describe("subDays doric function with column") {
    import spark.implicits._

    val df = List(
      (Timestamp.valueOf(now), Some(1)),
      (Timestamp.valueOf(now), Some(-1)),
      (Timestamp.valueOf(now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("timestampCol", "monthCol")

    it("should work as spark date_sub function with column") {
      df.testColumns2("timestampCol", "monthCol")(
        (d, m) => colTimestamp(d).subDays(colInt(m)),
        (d, m) => f.date_sub(f.col(d), f.col(m)),
        List(
          Date.valueOf(now.minusDays(1).toLocalDate),
          Date.valueOf(now.plusDays(1).toLocalDate),
          null,
          null,
          null
        ).map(Option(_))
      )
    }
  }
}
