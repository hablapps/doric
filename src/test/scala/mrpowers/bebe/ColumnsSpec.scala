package mrpowers.bebe

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.Extensions._
import mrpowers.bebe.Columns._
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class ColumnsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  it("allows for type safe programming") {

    val df = Seq(
      ("2012-05-05 12:01:15".t),
      ("2012-01-01 09:06:15".t),
      (null)
    ).toDF("some_time")

    val res = df
      .withColumn("hour", TimestampColumn("some_time").hour.col)
      .withColumn("end_of_month", TimestampColumn("some_time").to_date.end_of_month.col)

    res.show()
    res.printSchema()
    res.explain(true)

    val res2 = df
      .withColumn("hour", "some_time".tc.hour.col)
      .withColumn("end_of_month", "some_time".tc.to_date.end_of_month.col)

    res2.show()
    res2.printSchema()
    res2.explain(true)

    val res3 = df.withColumn("end_of_month", last_day(col("some_time")))

    res3.show()
    res3.printSchema()

    //      assertColumnEquality(df, "actual", "expected")

  }

  it("demonstrates how regular functions aren't type safe") {

    val df = Seq(
      (1),
      (2),
      (3)
    ).toDF("some_int")

    df.show()

//  df
//    .withColumn("hour", hour(col("some_int")))
//    .show()

//    val res = df.withColumn("hour", "some_int".ic.hour)

  }

}
