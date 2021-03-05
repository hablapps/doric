package org.apache.spark.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.BebeFunctions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.SparkSessionTestWrapper
import mrpowers.bebe.Extensions._
import java.sql.{Date, Timestamp}

class BebeFunctionsSpec extends FunSpec with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._

  describe("regexp_extract_all") {
    it("extracts multiple results") {
      val df = Seq(
        ("this 23 has 44 numbers", Array("23", "44")),
        ("no numbers", Array.empty[String]),
        (null, null)
      ).toDF("some_string", "expected")
        .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))
      assertColumnEquality(df, "actual", "expected")
    }
  }

  describe("beginning_of_month") {
//    it("has a good blog post example") {
//      val df = Seq(
//        (Date.valueOf("2020-01-15")),
//        (Date.valueOf("2020-01-20")),
//        (null)
//      ).toDF("some_date")
//        .withColumn("beginning_of_month", bebe_beginning_of_month(col("some_date")))
//
//      df.show()
//      df.explain(true)
//
//      val df = Seq(
//        (Date.valueOf("2020-01-15")),
//        (Date.valueOf("2020-01-20")),
//        (null)
//      ).toDF("some_date")
//        .withColumn("end_of_month", last_day(col("some_date")))
//
//      df.show()
//      df.explain(true)
//    }

    it("gets the beginning of the month of a date column") {
      val df = Seq(
        (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-01")),
        (Date.valueOf("2020-01-20"), Date.valueOf("2020-01-01")),
        (null, null)
      ).toDF("some_date", "expected")
        .withColumn("actual", bebe_beginning_of_month(col("some_date")))
      assertColumnEquality(df, "actual", "expected")
    }

    it("gets the beginning of the month of a timestamp column") {
      val df = Seq(
        (Timestamp.valueOf("2020-01-15 08:01:32"), Date.valueOf("2020-01-01")),
        (Timestamp.valueOf("2020-01-20 23:03:22"), Date.valueOf("2020-01-01")),
        (null, null)
      ).toDF("some_time", "expected")
        .withColumn("actual", bebe_beginning_of_month(col("some_time")))
      assertColumnEquality(df, "actual", "expected")
    }
  }

}
