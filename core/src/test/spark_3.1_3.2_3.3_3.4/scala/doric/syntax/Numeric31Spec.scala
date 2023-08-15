package doric
package syntax

import scala.reflect.ClassTag

import doric.Equalities._
import doric.types.NumericType
import doric.types.SparkType.Primitive
import java.sql.Timestamp
import org.scalactic.Equality
import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}

trait NumericOperations31Spec
    extends AnyFunSpecLike
    with TypedColumnTest
    with NumericUtilsSpec {

  def df: DataFrame

  import scala.reflect.runtime.universe._
  def test[T: NumericType: Primitive: ClassTag: TypeTag: Equality]()(implicit
      spark: SparkSession,
      fun: FromInt[T]
  ): Unit = {

    val numTypeStr = getClassName[T]

    describe(s"Numeric $numTypeStr") {

      it(s"acosh function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(0.0), Some(1.31696), None),
          _.acosh,
          f.acosh
        )
      }

      it(s"asinh function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.88137), Some(0.88137), Some(1.44364), None),
          _.asinh,
          f.asinh
        )
      }
    }
  }

  def testDecimals[
      T: NumWithDecimalsType: Primitive: ClassTag: TypeTag: Equality
  ]()(implicit
      spark: SparkSession,
      fun: FromFloat[T]
  ): Unit = {
    val numTypeStr = getClassName[T]

    describe(s"Num with Decimals $numTypeStr") {
      it(s"atanh function $numTypeStr") {
        testDoricSparkDecimals[T, Double](
          List(Some(-0.2f), Some(0.4f), Some(0.0f), None),
          List(Some(-0.20273), Some(0.423649), Some(0.0), None),
          _.atanh,
          f.atanh
        )
      }

      it(s"round function with scale and round mode") {
        val scale = 2
        val mode  = BigDecimal.RoundingMode.FLOOR
        testOnlyDoricDecimals[T, T](
          List(Some(1.466666f), Some(0.7111111f), Some(1.0f), None),
          List(Some(1.46f), Some(0.71f), Some(1.0f), None),
          _.round(scale.lit, mode)
        )
      }
    }
  }
}

class Numeric31Spec
    extends SparkSessionTestWrapper
    with NumericOperations31Spec {

  import spark.implicits._

  implicit val sparkSession: SparkSession = spark

  def df: DataFrame =
    List((1, 2f, 3L, 4.toDouble)).toDF(
      getName[Int](),
      getName[Float](),
      getName[Long](),
      getName[Double]()
    )

  test[Int]()
  test[Float]()
  test[Long]()
  test[Double]()

  testDecimals[Float]()
  testDecimals[Double]()

  describe("timestampSeconds doric function") {
    import spark.implicits._

    it("should work as spark timestamp_seconds function with integers") {
      val df = List(Some(123), Some(1), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colInt(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01")),
          None
        )
      )
    }

    it("should work as spark timestamp_seconds function with longs") {
      val df = List(Some(123L), Some(1L), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colLong(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01")),
          None
        )
      )
    }

    it("should work as spark timestamp_seconds function with doubles") {
      val df = List(Some(123.2), Some(1.9), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colDouble(c).timestampSeconds,
        c => f.timestamp_seconds(f.col(c)),
        List(
          Some(Timestamp.valueOf("1970-01-01 00:02:03.2")),
          Some(Timestamp.valueOf("1970-01-01 00:00:01.9")),
          None
        )
      )
    }
  }
}
