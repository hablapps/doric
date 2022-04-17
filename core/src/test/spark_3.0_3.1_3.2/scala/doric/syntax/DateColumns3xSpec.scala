package doric
package syntax

import java.sql.Date
import java.time.LocalDate
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class DateColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("addMonths doric function with column") {
    import spark.implicits._

    val df = List(
      (Date.valueOf(LocalDate.now), Some(1)),
      (Date.valueOf(LocalDate.now), Some(-1)),
      (Date.valueOf(LocalDate.now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("dateCol", "monthCol")

    it("should work as spark add_months function with column") {
      df.testColumns2("dateCol", "monthCol")(
        (d, m) => colDate(d).addMonths(colInt(m)),
        (d, m) => f.add_months(f.col(d), f.col(m)),
        List(
          Date.valueOf(LocalDate.now.plusMonths(1)),
          Date.valueOf(LocalDate.now.minusMonths(1)),
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
      (Date.valueOf(LocalDate.now), Some(1)),
      (Date.valueOf(LocalDate.now), Some(-1)),
      (Date.valueOf(LocalDate.now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("dateCol", "monthCol")

    it("should work as spark date_add function with column") {
      df.testColumns2("dateCol", "monthCol")(
        (d, m) => colDate(d).addDays(colInt(m)),
        (d, m) => f.date_add(f.col(d), f.col(m)),
        List(
          Date.valueOf(LocalDate.now.plusDays(1)),
          Date.valueOf(LocalDate.now.minusDays(1)),
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
      (Date.valueOf(LocalDate.now), Some(1)),
      (Date.valueOf(LocalDate.now), Some(-1)),
      (Date.valueOf(LocalDate.now), None),
      (null, Some(1)),
      (null, None)
    ).toDF("dateCol", "monthCol")

    it("should work as spark date_sub function with column") {
      df.testColumns2("dateCol", "monthCol")(
        (d, m) => colDate(d).subDays(colInt(m)),
        (d, m) => f.date_sub(f.col(d), f.col(m)),
        List(
          Date.valueOf(LocalDate.now.minusDays(1)),
          Date.valueOf(LocalDate.now.plusDays(1)),
          null,
          null,
          null
        ).map(Option(_))
      )
    }
  }
}
