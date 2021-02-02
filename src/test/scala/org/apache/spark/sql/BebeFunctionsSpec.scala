package org.apache.spark.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.BebeFunctions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.SparkSessionTestWrapper

class BebeFunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  describe("regexp_extract_all") {

    it("extracts multiple results") {

      val df = Seq(
        ("this 23 has 44 numbers"),
        ("no numbers"),
        (null)
      ).toDF("some_string")

      df.show(false)

      val res = df
        .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))

      res.show(false)
      res.printSchema()

//      assertColumnEquality(df, "actual", "expected")

    }

  }

}
