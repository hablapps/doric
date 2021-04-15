package habla.doric

import habla.doric.Extensions._

import org.apache.spark.sql.types.{StringType, FloatType, LongType, DoubleType}

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.scalatest.funspec.AnyFunSpec
import Predef.{any2stringadd => _, _} // scalafix:ok
import java.sql.Timestamp

class DoricColumnsSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with ColumnComparer
    with TypedColumnTest {

  // scalafix:ok
  import spark.implicits._

  it("allows for type safe programming") {
    val df = Seq(
      (Some("2012-05-05 12:01:15".t), Some(12), Some("2012-08-31".d)),
      (Some("2012-01-01 09:06:15".t), Some(9), Some("2012-04-30".d)),
      (None, None, None)
    ).toDF("some_time", "expected_hour", "expected_end_of_month")
    val res = df
      .withColumn("hour", getTimestamp("some_time").hour withTypeChecked)
      .withColumn(
        "end_of_month",
        getTimestamp("some_time").toDate.withTypeChecked
          .addMonths(3)
          .withTypeChecked
          .endOfMonth withTypeChecked
      )
    assertColumnEquality(res, "hour", "expected_hour")
    assertColumnEquality(res, "end_of_month", "expected_end_of_month")
  }

  it("should use the numeric functions") {
    val df = Seq(
      (2, true, 1, 4, 4),
      (3, true, 2, 6, 5),
      (4, false, 3, 8, 6)
    ).toDF("some_data", "expected_result", "expected_minus", "expected_double", "expected_sum")
      .withColumn("transformed", getInt("some_data") <= 3 withTypeChecked)
      .withColumn("minus", getInt("some_data") - 1 withTypeChecked)
      .withColumn("plus_double", getInt("some_data") + getInt("some_data") withTypeChecked)
      .withColumn("mult_double", getInt("some_data") * 2 withTypeChecked)
      .withColumn("sum", getInt("some_data") + 2 withTypeChecked)

    assertColumnEquality(df, "transformed", "expected_result")
    assertColumnEquality(df, "minus", "expected_minus")
    assertColumnEquality(df, "plus_double", "expected_double")
    assertColumnEquality(df, "mult_double", "expected_double")
    assertColumnEquality(df, "sum", "expected_sum")
  }

  it("should cast to the valid types") {
    val df = List(1)
      .map(x => (x, x.toString, x.toFloat, x.toLong, x.toDouble))
      .toDF("some_data", "expected_string", "expected_float", "expected_long", "expected_double")
      .withColumn("transformed_string", getInt("some_data").testCastingTo[String](StringType))
      .withColumn("transformed_float", getInt("some_data").testCastingTo[Float](FloatType))
      .withColumn("transformed_long", getInt("some_data").testCastingTo[Long](LongType))
      .withColumn("transformed_double", getInt("some_data").testCastingTo[Double](DoubleType))

    assertColumnEquality(df, "transformed_string", "expected_string")
    assertColumnEquality(df, "transformed_float", "expected_float")
    assertColumnEquality(df, "transformed_long", "expected_long")
    assertColumnEquality(df, "transformed_double", "expected_double")
  }
}
