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
      .withColumn("end_of_month")(_.get[TimestampColumn]("some_time").to_date.add_months(3.tc).end_of_month)
    assertColumnEquality(res, "hour", "expected_hour")
    assertColumnEquality(res, "end_of_month", "expected_end_of_month")
  }

  it("should use the numeric functions") {
    val df = Seq(
      (2, true, 1, 4),
      (3, true, 2, 6),
      (4, false, 3, 8)
    ).toDF("some_data", "expected_result", "expected_minus", "expected_double")
      .withColumn("transformed")(_.get[IntegerColumn]("some_data") <= 3)
      .withColumn("minus")(_.get[IntegerColumn]("some_data") - 1)
      .withColumn("plus_double")(df => df.get[IntegerColumn]("some_data") + df.get[IntegerColumn]("some_data"))
      .withColumn("mult_double")(df => df.get[IntegerColumn]("some_data") * 2)

    assertColumnEquality(df, "transformed", "expected_result")
    assertColumnEquality(df, "minus", "expected_minus")
    assertColumnEquality(df, "plus_double", "expected_double")
    assertColumnEquality(df, "mult_double", "expected_double")
  }
}
