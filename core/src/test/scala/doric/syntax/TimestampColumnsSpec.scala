package doric
package syntax

import doric.Equalities._
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class TimestampColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  val now: LocalDateTime = LocalDateTime.now

  describe("currentTimestamp doric function") {
    import spark.implicits._

    val df = List(("1", "1")).toDF("col1", "col2")

    it("should work as spark current_timestamp function") {
      df.testColumn(currentTimestamp(), f.current_timestamp())
    }
  }

  describe("addMonths doric function with literal") {
    import spark.implicits._

    val df = List(Timestamp.valueOf(now), null).toDF("timestampCol")

    it("should work as spark add_months function with literal") {
      df.testColumns2("timestampCol", 3)(
        (d, m) => colTimestamp(d).addMonths(m.lit),
        (d, m) => f.add_months(f.col(d), m),
        List(Date.valueOf(now.plusMonths(3).toLocalDate), null).map(Option(_))
      )
    }

    it("should subtract months if num months < 0 with literal") {
      df.testColumns2("timestampCol", -3)(
        (d, m) => colTimestamp(d).addMonths(m.lit),
        (d, m) => f.add_months(f.col(d), m),
        List(Date.valueOf(now.minusMonths(3).toLocalDate), null).map(Option(_))
      )
    }
  }

  describe("fromUtc doric function") {
    import spark.implicits._

    val df = List(Timestamp.valueOf("2017-07-14 02:40:00"), null)
      .toDF("timestampCol")

    it("should work as spark from_utc_timestamp function") {
      df.testColumns2("timestampCol", "GMT+1")(
        (d, m) => colTimestamp(d).fromUtc(m.lit),
        (d, m) => f.from_utc_timestamp(f.col(d), m),
        List(Timestamp.valueOf("2017-07-14 03:40:00"), null).map(Option(_))
      )
    }

    if (spark.version.take(1) == "3") {
      it("should fail if invalid timeZone") {
        intercept[java.time.DateTimeException](
          df.select(colTimestamp("timestampCol").fromUtc("wrong timeZone".lit))
            .collect()
        )
      }
    }
  }

  describe("toUtc doric function") {
    import spark.implicits._

    val df = List(Timestamp.valueOf("2017-07-14 02:40:00"), null)
      .toDF("timestampCol")

    it("should work as spark to_utc_timestamp function") {
      df.testColumns2("timestampCol", "GMT+1")(
        (d, m) => colTimestamp(d).toUtc(m.lit),
        (d, m) => f.to_utc_timestamp(f.col(d), m),
        List(Timestamp.valueOf("2017-07-14 01:40:00"), null).map(Option(_))
      )
    }

    if (spark.version.take(1) == "3") {
      it("should fail if invalid timeZone") {
        intercept[java.time.DateTimeException](
          df.select(colTimestamp("timestampCol").toUtc("wrong timeZone".lit))
            .collect()
        )
      }
    }
  }

  describe("addDays doric function with literal") {
    import spark.implicits._

    val df = List(Timestamp.valueOf(now), null).toDF("timestampCol")

    it("should work as spark date_add function with literal") {
      df.testColumns2("timestampCol", 3)(
        (d, m) => colTimestamp(d).addDays(m.lit),
        (d, m) => f.date_add(f.col(d), m),
        List(Date.valueOf(now.plusDays(3).toLocalDate), null).map(Option(_))
      )
    }

    it("should subtract months if num months < 0 with literal") {
      df.testColumns2("timestampCol", -3)(
        (d, m) => colTimestamp(d).addDays(m.lit),
        (d, m) => f.date_add(f.col(d), m),
        List(Date.valueOf(now.minusDays(3).toLocalDate), null).map(Option(_))
      )
    }
  }

  describe("format doric function with literal") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-05 00:00:00"), null).toDF("timestampCol")

    it("should work as spark date_format function with literal") {
      df.testColumns2("timestampCol", "dd.MM.yyyy H:m:s")(
        (d, m) => colTimestamp(d).format(m.lit),
        (d, m) => f.date_format(f.col(d), m),
        List("05.10.2021 0:0:0", null).map(Option(_))
      )
    }

    it("should throw an exception if malformed format") {
      intercept[java.lang.IllegalArgumentException] {
        df.withColumn("test", colTimestamp("timestampCol").format("nnn".lit))
          .collect()
      }
    }
  }

  describe("subDays doric function with literal") {
    import spark.implicits._

    val df = List(Timestamp.valueOf(now), null).toDF("timestampCol")

    it("should work as spark date_sub function with literal") {
      df.testColumns2("timestampCol", 3)(
        (d, m) => colTimestamp(d).subDays(m.lit),
        (d, m) => f.date_sub(f.col(d), m),
        List(Date.valueOf(now.minusDays(3).toLocalDate), null).map(Option(_))
      )
    }

    it("should add months if num months < 0 with literal") {
      df.testColumns2("timestampCol", -3)(
        (d, m) => colTimestamp(d).subDays(m.lit),
        (d, m) => f.date_sub(f.col(d), m),
        List(Date.valueOf(now.plusDays(3).toLocalDate), null).map(Option(_))
      )
    }
  }

  describe("diff doric function") {
    import spark.implicits._

    val df = List(
      (
        Timestamp.valueOf("2021-10-05 00:00:00"),
        Timestamp.valueOf("2021-10-06 00:00:00")
      ),
      (
        Timestamp.valueOf("2021-10-06 00:00:00"),
        Timestamp.valueOf("2021-10-05 00:00:00")
      ),
      (Timestamp.valueOf(now), null),
      (null, Timestamp.valueOf(now)),
      (null, null)
    ).toDF("timestampCol", "date2Col")

    it("should work as spark datediff function") {
      df.testColumns2("timestampCol", "date2Col")(
        (d, m) => colTimestamp(d).diff(colTimestamp(m)),
        (d, m) => f.datediff(f.col(d), f.col(m)),
        List(Some(-1), Some(1), None, None, None)
      )
    }
  }

  describe("dayOfMonth doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark dayofmonth function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).dayOfMonth,
        d => f.dayofmonth(f.col(d)),
        List(Some(21), None)
      )
    }
  }

  describe("dayOfWeek doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark dayofweek function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).dayOfWeek,
        d => f.dayofweek(f.col(d)),
        List(Some(5), None)
      )
    }
  }

  describe("dayOfYear doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark dayofyear function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).dayOfYear,
        d => f.dayofyear(f.col(d)),
        List(Some(294), None)
      )
    }
  }

  describe("endOfMonth doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark last_day function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).endOfMonth,
        d => f.last_day(f.col(d)),
        List(Date.valueOf("2021-10-31"), null).map(Option(_))
      )
    }
  }

  describe("lastDayOfMonth doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark last_day function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).lastDayOfMonth,
        d => f.last_day(f.col(d)),
        List(Date.valueOf("2021-10-31"), null).map(Option(_))
      )
    }
  }

  describe("month doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark month function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).month,
        d => f.month(f.col(d)),
        List(Some(10), None)
      )
    }
  }

  describe("monthsBetween doric function") {
    import spark.implicits._

    val df = List(
      (
        Timestamp.valueOf("2017-07-14 00:00:00"),
        Timestamp.valueOf("2017-11-14 00:00:00")
      ),
      (
        Timestamp.valueOf("2017-11-14 00:00:00"),
        Timestamp.valueOf("2017-07-14 00:00:00")
      ),
      (
        Timestamp.valueOf("2017-01-01 00:00:00"),
        Timestamp.valueOf("2017-01-10 00:00:00")
      ),
      (Timestamp.valueOf(now), null),
      (null, Timestamp.valueOf(now)),
      (null, null)
    ).toDF("timestampCol", "date2Col")

    it("should work as spark months_between function") {
      df.testColumns2("timestampCol", "date2Col")(
        (d, m) => colTimestamp(d).monthsBetween(colTimestamp(m)),
        (d, m) => f.months_between(f.col(d), f.col(m)),
        List(Some(-4.0), Some(4.0), Some(-0.29032258), None, None, None)
      )
    }

    it("should work as spark months_between function with roundOff param") {
      df.testColumns3("timestampCol", "date2Col", false)(
        (d, m, r) => colTimestamp(d).monthsBetween(colTimestamp(m), r.lit),
        (d, m, r) => f.months_between(f.col(d), f.col(m), r),
        List(Some(-4.0), Some(4.0), Some(-0.2903225806451613), None, None, None)
      )
    }
  }

  describe("nextDay doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-05 00:00:00"), null).toDF("timestampCol")

    it("should work as spark next_day function") {
      df.testColumns2("timestampCol", "Sun")(
        (date, day) => colTimestamp(date).nextDay(day.lit),
        (date, day) => f.next_day(f.col(date), day),
        List(Date.valueOf("2021-10-10"), null).map(Option(_))
      )
    }
  }

  describe("quarter doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark quarter function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).quarter,
        d => f.quarter(f.col(d)),
        List(Some(4), None)
      )
    }
  }

  describe("trunc doric function with literal") {
    import spark.implicits._

    val df = List(Timestamp.valueOf("2021-10-05 00:00:00"), null)
      .toDF("timestampCol")

    it("should work as spark date_trunc function with literal") {
      df.testColumns2("timestampCol", "yyyy")(
        (d, m) => colTimestamp(d).truncate(m.lit),
        (d, m) => f.date_trunc(m, f.col(d)),
        List(Timestamp.valueOf("2021-01-01 00:00:00"), null).map(Option(_))
      )
    }

    it("should return null if malformed format") {
      df.testColumns2("timestampCol", "yabcd")(
        (d, m) => colTimestamp(d).truncate(m.lit),
        (d, m) => f.date_trunc(m, f.col(d)),
        List(null, null).map(Option(_))
      )
    }
  }

  describe("weekOfYear doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark weekofyear function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).weekOfYear,
        d => f.weekofyear(f.col(d)),
        List(Some(42), None)
      )
    }
  }

  describe("year doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("timestampCol")

    it("should work as spark year function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).year,
        d => f.year(f.col(d)),
        List(Some(2021), None)
      )
    }
  }

  describe("second doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 12:13:14"), null).toDF("timestampCol")

    it("should work as spark second function") {
      df.testColumns("timestampCol")(
        d => colTimestamp(d).second,
        d => f.second(f.col(d)),
        List(Some(14), None)
      )
    }
  }

  describe("toDate doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("dateCol")

    it("should work as spark to_date function") {
      df.testColumns("dateCol")(
        d => colTimestamp(d).toDate,
        d => f.to_date(f.col(d)),
        List(Date.valueOf("2021-10-21"), null).map(Option(_))
      )
    }
  }

  describe("toLocalDate doric function") {
    import spark.implicits._

    val df =
      List(Timestamp.valueOf("2021-10-21 00:00:00"), null).toDF("dateCol")

    it("should work as spark to_date function") {
      df.testColumns("dateCol")(
        d => colTimestamp(d).toLocalDate,
        d => f.to_date(f.col(d)),
        List(LocalDate.parse("2021-10-21"), null).map(Option(_))
      )
    }
  }
}
