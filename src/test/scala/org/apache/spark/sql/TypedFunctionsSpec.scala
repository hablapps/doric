package org.apache.spark.sql

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.Extensions._
import mrpowers.bebe.{SparkSessionTestWrapper, _}
import org.scalatest.FunSpec

import org.apache.spark.sql.TypedFunctions._

class TypedFunctionsSpec
  extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  describe("add_months") {

    it("adds months to a date") {
      val df = Seq(
        (Some("2020-01-05".d), 2, Some("2020-03-05".d)),
        (Some("2019-04-13".d), 3, Some("2019-07-13".d)),
        (None, 4, None)
      ).toDF("some_date", "months", "expected")
      val months: IntegerColumn = df.get[IntegerColumn]("months")
      val res = df.withColumn("actual", add_months(df.get[DateColumn]("some_date"), months))
      assertColumnEquality(res, "actual", "expected")
    }

    it("adds a literal month value") {
      val df = Seq(
        (Some("2020-01-05".d), Some("2020-03-05".d)),
        (Some("2019-04-13".d), Some("2019-06-13".d)),
        (None, None)
      ).toDF("some_date", "expected")
      val months: IntegerColumn = 2.tc
      val res = df.withColumn("actual", add_months(df.get[DateColumn]("some_date"), months))
      assertColumnEquality(res, "actual", "expected")
    }

    it("validates the type of the column in runtime") {
      val df = Seq(
        (Some("2020-01-05".d), 2),
        (Some("2019-04-13".d), 3),
        (None, 4)
      ).toDF("some_date", "months")

      df.get[DateColumn]("some_date")

      intercept[Exception] {
        df.get[IntegerColumn]("some_date")
      }
    }

  }

}
