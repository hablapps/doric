package org.apache.spark.sql

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.Columns.{DateColumn, IntegerColumn}
import mrpowers.bebe.SparkSessionTestWrapper
import org.scalatest.FunSpec
import mrpowers.bebe.Extensions._

import org.apache.spark.sql.TypedFunctions._

class TypedFunctionsSpec
  extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  describe("add_months") {

    it("adds months to a date") {

      val df = Seq(
        ("2020-01-05".d, 2),
        ("2019-04-13".d, 3),
        (null, 4)
      ).toDF("some_date", "months")

      val res = df.withColumn("plus_months", add_months("some_date".dc, "months".ic).col)

      res.show()
      res.printSchema()

    }

    it("adds a literal month value") {

      val df = Seq(
        ("2020-01-05".d),
        ("2019-04-13".d),
        (null)
      ).toDF("some_date")

      val res = df.withColumn("plus_months", add_months("some_date".dc, 2.il).col)

      res.show()
      res.printSchema()

    }

    it("validates the type of the column in runtime") {
      val df = Seq(
        ("2020-01-05".d, 2),
        ("2019-04-13".d, 3),
        (null, 4)
      ).toDF("some_date", "months")

      df.get[DateColumn]("some_date")

      intercept[Exception] {
        df.get[IntegerColumn]("some_date")
      }
    }

  }

}
