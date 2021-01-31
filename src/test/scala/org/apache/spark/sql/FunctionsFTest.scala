package org.apache.spark.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer

class FunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  describe("regexp_extract_all") {

    it("extracts multiple results") {

      val data = Seq(
        ("this 23 has 44 numbers"),
        ("no numbers"),
        (null)
      )

      val df = data
        .toDF("some_string")
        .withColumn("actual", functionsf.regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))

      df.show()

//      assertColumnEquality(df, "actual", "expected")

    }

  }

}
