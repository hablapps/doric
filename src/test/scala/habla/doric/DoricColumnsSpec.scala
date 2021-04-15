package habla.doric

import scala.Predef.{any2stringadd => _}

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import habla.doric.Extensions._
import java.sql.{Date, Timestamp}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StringType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DoricColumnsSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with ColumnComparer
    with TypedColumnTest
    with Matchers
    with EitherValues {

  // scalafix:ok
  import spark.implicits._

  Seq((1, 2), (3, 4)).toDF("c1", "c2")

  def testValue[T: FromDf](df: DataFrame): Unit = {
    get[T]("column").elem.run(df).toEither.value
  }

  describe("each column should represent their datatype") {
    it("works for String") {
      testValue[String](List("hola").toDF("column"))
      testValue[String](List(Some("hola"), None).toDF("column"))
    }
    it("works for Int") {
      testValue[Int](List(14).toDF("column"))
      testValue[Int](List(Some(54), None).toDF("column"))
    }
    it("works for Long") {
      testValue[Long](List(14L).toDF("column"))
      testValue[Long](List(Some(54L), None).toDF("column"))
    }
    it("works for Float") {
      testValue[Float](List(14f).toDF("column"))
      testValue[Float](List(Some(54f), None).toDF("column"))
    }
    it("works for Array") {
      testValue[Array[Int]](List(List(14)).toDF("column"))
      testValue[Array[Int]](List(Some(List(54)), None).toDF("column"))
      testValue[Array[Long]](List(List(14L)).toDF("column"))
      testValue[Array[Long]](List(Some(List(54L)), None).toDF("column"))
    }
    it("works for Date") {
      testValue[Date](List(Date.valueOf("2020-01-01")).toDF("column"))
      testValue[Date](List(Some(Date.valueOf("2020-01-01")), None).toDF("column"))
    }
    val timestamp = Timestamp.valueOf("2020-01-01 01:01:901")
    it("works for Timestamp") {
      testValue[Timestamp](List(timestamp).toDF("column"))
      testValue[Timestamp](List(Some(timestamp), None).toDF("column"))
    }

    it("works for Map") {
      testValue[Map[Timestamp, Int]](List(Map(timestamp -> 10)).toDF("column"))
      testValue[Map[Timestamp, Int]](List(Some(Map(timestamp -> 10)), None).toDF("column"))
      List((Map("yepe2" -> 10), "yepe2"))
        .toDF("column", "key")
        .withColumn("hola", col("column")(col("key")))
        .show
    }
  }

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
