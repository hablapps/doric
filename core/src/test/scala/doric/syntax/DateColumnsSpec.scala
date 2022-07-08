package doric
package syntax

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, functions => f}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.dateAddMonths

class DateColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers
    with DateColumnTest {

  import spark.implicits._

  def df: DataFrame =
    List(
      (
        Date.valueOf("2022-05-27"),
        new Timestamp(1653669106840L)
      )
    ).toDF(
      getName("date"),
      getName("timestamp")
    )

  test[Date]("date")
  test[Timestamp]("timestamp")

  describe("currentDate doric function") {
    import spark.implicits._

    val df = List(("1", "1")).toDF("col1", "col2")

    it("should work as spark current_date function") {
      df.testColumn(
        currentDate(),
        f.current_date(),
        List(Date.valueOf(LocalDate.now)).map(Option(_))
      )

      df.testColumn(
        currentDateT[LocalDate](),
        f.current_date(),
        List(LocalDate.now).map(Option(_))
      )
    }
  }

  describe("addMonths doric function with literal") {
    import spark.implicits._

    val df = List(Date.valueOf(LocalDate.now), null).toDF("dateCol")

    it("should work as spark add_months function with literal") {
      df.testColumns2("dateCol", 3)(
        (d, m) => colDate(d).addMonths(m.lit),
        (d, m) => f.add_months(f.col(d), m),
        List(Date.valueOf(LocalDate.now.plusMonths(3)), null).map(Option(_))
      )
    }

    it("should subtract months if num months < 0 with literal") {
      val additionalDays: Int =
        dateAddMonths(LocalDate.now().toEpochDay.asInstanceOf[Int], -3)
      val expectedDate =
        LocalDate.of(1970, 1, 1).plusDays(additionalDays.toLong)

      df.testColumns2("dateCol", -3)(
        (d, m) => colDate(d).addMonths(m.lit),
        (d, m) => f.add_months(f.col(d), m),
        List(Date.valueOf(expectedDate), null).map(Option(_))
      )
    }

    it(
      "should correctly subtract months if num months < 0 with literal for end of month dates"
    ) {
      val localDate = LocalDate.of(2022, 6, 30)

      val df = List(Date.valueOf(localDate), null).toDF("dateCol")

      val additionalDays: Int =
        dateAddMonths(localDate.toEpochDay.asInstanceOf[Int], -3)
      val expectedDate =
        LocalDate.of(1970, 1, 1).plusDays(additionalDays.toLong)

      df.testColumns2("dateCol", -3)(
        (d, m) => colDate(d).addMonths(m.lit),
        (d, m) => f.add_months(f.col(d), m),
        List(Date.valueOf(expectedDate), null).map(Option(_))
      )
    }
  }

  describe("addDays doric function with literal") {
    import spark.implicits._

    val df = List(Date.valueOf(LocalDate.now), null).toDF("dateCol")

    it("should work as spark date_add function with literal") {
      df.testColumns2("dateCol", 3)(
        (d, m) => colDate(d).addDays(m.lit),
        (d, m) => f.date_add(f.col(d), m),
        List(Date.valueOf(LocalDate.now.plusDays(3)), null).map(Option(_))
      )
    }

    it("should subtract months if num months < 0 with literal") {
      df.testColumns2("dateCol", -3)(
        (d, m) => colDate(d).addDays(m.lit),
        (d, m) => f.date_add(f.col(d), m),
        List(Date.valueOf(LocalDate.now.minusDays(3)), null).map(Option(_))
      )
    }
  }

  describe("format doric function with literal") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-05"), null).toDF("dateCol")

    it("should work as spark date_format function with literal") {
      df.testColumns2("dateCol", "dd.MM.yyyy")(
        (d, m) => colDate(d).format(m.lit),
        (d, m) => f.date_format(f.col(d), m),
        List("05.10.2021", null).map(Option(_))
      )
    }

    it("should throw an exception if malformed format") {
      intercept[java.lang.IllegalArgumentException] {
        df.withColumn("test", colDate("dateCol").format("nnn".lit))
          .collect()
      }
    }
  }

  describe("subDays doric function with literal") {
    import spark.implicits._

    val df = List(Date.valueOf(LocalDate.now), null).toDF("dateCol")

    it("should work as spark date_sub function with literal") {
      df.testColumns2("dateCol", 3)(
        (d, m) => colDate(d).subDays(m.lit),
        (d, m) => f.date_sub(f.col(d), m),
        List(Date.valueOf(LocalDate.now.minusDays(3)), null).map(Option(_))
      )
    }

    it("should add months if num months < 0 with literal") {
      df.testColumns2("dateCol", -3)(
        (d, m) => colDate(d).subDays(m.lit),
        (d, m) => f.date_sub(f.col(d), m),
        List(Date.valueOf(LocalDate.now.plusDays(3)), null).map(Option(_))
      )
    }
  }

  describe("diff doric function") {
    import spark.implicits._

    val df = List(
      (Date.valueOf("2021-10-05"), Date.valueOf("2021-10-06")),
      (Date.valueOf("2021-10-06"), Date.valueOf("2021-10-05")),
      (Date.valueOf(LocalDate.now), null),
      (null, Date.valueOf(LocalDate.now)),
      (null, null)
    ).toDF("dateCol", "date2Col")

    it("should work as spark datediff function") {
      df.testColumns2("dateCol", "date2Col")(
        (d, m) => colDate(d).diff(colDate(m)),
        (d, m) => f.datediff(f.col(d), f.col(m)),
        List(Some(-1), Some(1), None, None, None)
      )
    }
  }

  describe("dayOfMonth doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark dayofmonth function") {
      df.testColumns("dateCol")(
        d => colDate(d).dayOfMonth,
        d => f.dayofmonth(f.col(d)),
        List(Some(21), None)
      )
    }
  }

  describe("dayOfWeek doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark dayofweek function") {
      df.testColumns("dateCol")(
        d => colDate(d).dayOfWeek,
        d => f.dayofweek(f.col(d)),
        List(Some(5), None)
      )
    }
  }

  describe("dayOfYear doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark dayofyear function") {
      df.testColumns("dateCol")(
        d => colDate(d).dayOfYear,
        d => f.dayofyear(f.col(d)),
        List(Some(294), None)
      )
    }
  }

  describe("endOfMonth doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark last_day function") {
      df.testColumns("dateCol")(
        d => colDate(d).endOfMonth,
        d => f.last_day(f.col(d)),
        List(Date.valueOf("2021-10-31"), null).map(Option(_))
      )
    }
  }

  describe("lastDayOfMonth doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark last_day function") {
      df.testColumns("dateCol")(
        d => colDate(d).lastDayOfMonth,
        d => f.last_day(f.col(d)),
        List(Date.valueOf("2021-10-31"), null).map(Option(_))
      )
    }
  }

  describe("month doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark month function") {
      df.testColumns("dateCol")(
        d => colDate(d).month,
        d => f.month(f.col(d)),
        List(Some(10), None)
      )
    }
  }

  describe("monthsBetween doric function") {
    import spark.implicits._

    val df = List(
      (Date.valueOf("2017-07-14"), Date.valueOf("2017-11-14")),
      (Date.valueOf("2017-11-14"), Date.valueOf("2017-07-14")),
      (Date.valueOf("2017-01-01"), Date.valueOf("2017-01-10")),
      (Date.valueOf(LocalDate.now), null),
      (null, Date.valueOf(LocalDate.now)),
      (null, null)
    ).toDF("dateCol", "date2Col")

    it("should work as spark months_between function") {
      df.testColumns2("dateCol", "date2Col")(
        (d, m) => colDate(d).monthsBetween(colDate(m)),
        (d, m) => f.months_between(f.col(d), f.col(m)),
        List(Some(-4.0), Some(4.0), Some(-0.29032258), None, None, None)
      )
    }

    it("should work as spark months_between function with roundOff param") {
      df.testColumns3("dateCol", "date2Col", false)(
        (d, m, r) => colDate(d).monthsBetween(colDate(m), r.lit),
        (d, m, r) => f.months_between(f.col(d), f.col(m), r),
        List(Some(-4.0), Some(4.0), Some(-0.2903225806451613), None, None, None)
      )
    }
  }

  describe("nextDay doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-05"), null).toDF("dateCol")

    it("should work as spark next_day function") {
      df.testColumns2("dateCol", "Sun")(
        (date, day) => colDate(date).nextDay(day.lit),
        (date, day) => f.next_day(f.col(date), day),
        List(Date.valueOf("2021-10-10"), null).map(Option(_))
      )
    }
  }

  describe("quarter doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark quarter function") {
      df.testColumns("dateCol")(
        d => colDate(d).quarter,
        d => f.quarter(f.col(d)),
        List(Some(4), None)
      )
    }
  }

  describe("truncate doric function with literal") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-05"), null).toDF("dateCol")

    it("should work as spark trunc function with dates") {
      df.testColumns2("dateCol", "yyyy")(
        (d, m) => colDate(d).truncate(m.lit),
        (d, m) => f.trunc(f.col(d), m),
        List(Date.valueOf("2021-01-01"), null).map(Option(_))
      )
    }

    it("should return null if malformed format") {
      df.testColumns2("dateCol", "second")(
        (d, m) => colDate(d).truncate(m.lit),
        (d, m) => f.trunc(f.col(d), m),
        List(null, null).map(Option(_))
      )
    }
  }

  describe("date unixTimestamp doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-05"), null).toDF("dateCol")

    it("should work as spark unix_timestamp function") {
      df.testColumns("dateCol")(
        d => colDate(d).unixTimestamp,
        d => f.unix_timestamp(f.col(d)),
        List(Some(1633392000L), None)
      )
    }
  }

  describe("weekOfYear doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark weekofyear function") {
      df.testColumns("dateCol")(
        d => colDate(d).weekOfYear,
        d => f.weekofyear(f.col(d)),
        List(Some(42), None)
      )
    }
  }

  describe("year doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark year function") {
      df.testColumns("dateCol")(
        d => colDate(d).year,
        d => f.year(f.col(d)),
        List(Some(2021), None)
      )
    }
  }

  describe("toTimestamp doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark to_timestamp function") {
      df.testColumns("dateCol")(
        d => colDate(d).toTimestamp,
        d => f.to_timestamp(f.col(d)),
        List(Timestamp.valueOf("2021-10-21 00:00:00"), null).map(Option(_))
      )
    }
  }

  describe("toInstant doric function") {
    import spark.implicits._

    val df = List(Date.valueOf("2021-10-21"), null).toDF("dateCol")

    it("should work as spark to_timestamp function") {
      df.testColumns("dateCol")(
        d => colDate(d).toInstant,
        d => f.to_timestamp(f.col(d)),
        List(Instant.parse("2021-10-21T00:00:00Z"), null).map(Option(_))
      )
    }
  }
}
