package mrpowers.bebe

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import mrpowers.bebe.Extensions._
import org.scalatest.FunSpec

class BebeColumnsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  import spark.implicits._

  it("allows for type safe programming") {
    val df = Seq(
      (Some("2012-05-05 12:01:15".t), Some(12), Some("2012-08-31".d)),
      (Some("2012-01-01 09:06:15".t), Some(9), Some("2012-04-30".d)),
      (None, None, None)
    ).toDF("some_time", "expected_hour", "expected_end_of_month")
    val res = df
      .withColumn("hour", df.get[TimestampColumn]("some_time").hour)
      .withColumn("end_of_month")(_.get[TimestampColumn]("some_time").to_date.add_months(3.il).end_of_month)
    assertColumnEquality(res, "hour", "expected_hour")
    assertColumnEquality(res, "end_of_month", "expected_end_of_month")
  }

  it("should use the numeric functions") {
    val df = Seq(
      (2, true),
      (3, true),
      (4, false)
    ).toDF("some_data", "expected_result")
      .withColumn("transformed")(_.get[IntegerColumn]("some_data") <= 3.il)


    assertColumnEquality(df, "transformed", "expected_result")
  }
}
