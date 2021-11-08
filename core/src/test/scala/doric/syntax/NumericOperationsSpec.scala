package doric
package syntax

import scala.reflect.{ClassTag, classTag}
import doric.types.{NumericType, SparkType}
import org.scalatest.funspec.AnyFunSpecLike
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp

import doric.implicitConversions.stringCname

trait NumericOperationsSpec extends AnyFunSpecLike with TypedColumnTest {

  def df: DataFrame

  def test[T: NumericType: SparkType: ClassTag](): Unit = {

    describe(s"Numeric ${classTag[T].getClass.getSimpleName}") {

      it("+") {
        test[T, T, T]((a, b) => a + b)
      }
      it("-") {
        test[T, T, T]((a, b) => a - b)
      }
      it("*") {
        test[T, T, T]((a, b) => a * b)
      }
      it("/") {
        test[T, T, Double]((a, b) => a / b)
      }
      it("%") {
        test[T, T, T]((a, b) => a % b)
      }
      it(">") {
        test[T, T, Boolean]((a, b) => a > b)
      }
      it(">=") {
        test[T, T, Boolean]((a, b) => a >= b)
      }
      it("<") {
        test[T, T, Boolean]((a, b) => a < b)
      }
      it("<=") {
        test[T, T, Boolean]((a, b) => a <= b)
      }
    }
  }

  def test[T1: SparkType: ClassTag, T2: SparkType: ClassTag, O: SparkType](
      f: (DoricColumn[T1], DoricColumn[T2]) => DoricColumn[O]
  ): Unit =
    df.validateColumnType(
      f(
        col[T1](getName[T1]()),
        col[T2](getName[T1]())
      )
    )

  def getName[T: ClassTag](pos: Int = 1): String =
    s"col_${classTag[T].getClass.getSimpleName}_$pos"
}

class NumericSpec extends NumericOperationsSpec with SparkSessionTestWrapper {

  import spark.implicits._

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

  describe("formatNumber doric function") {
    import spark.implicits._

    it("should work as spark format_number function") {
      val df = List(Some(123.567), Some(1.0001), None)
        .toDF("col1")

      df.testColumns2("col1", 1)(
        (c, d) => colDouble(c).formatNumber(d.lit),
        (c, d) => f.format_number(f.col(c), d),
        List(Some("123.6"), Some("1.0"), None)
      )
    }
  }

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

  describe("fromUnixTime doric function") {
    import spark.implicits._

    it("should work as spark format_number function") {
      val df = List(Some(123L), Some(1L), None)
        .toDF("col1")

      df.testColumns("col1")(
        c => colLong(c).fromUnixTime,
        c => f.from_unixtime(f.col(c)),
        List(Some("1970-01-01 00:02:03"), Some("1970-01-01 00:00:01"), None)
      )
    }

    it("should work as spark format_number(pattern) function") {
      val df = List(Some(123L), Some(1L), None)
        .toDF("col1")

      df.testColumns2("col1", "yyyy-MM-dd h:m:s")(
        (c, p) => colLong(c).fromUnixTime(p.lit),
        (c, p) => f.from_unixtime(f.col(c), p),
        List(Some("1970-01-01 12:2:3"), Some("1970-01-01 12:0:1"), None)
      )
    }

    it("should fail if wrong pattern is given") {
      val df = List(Some(123L), Some(1L), None)
        .toDF("col1")

      intercept[IllegalArgumentException](
        df.select(colLong("col1").fromUnixTime("wrong pattern".lit))
          .collect()
      )
    }
  }

}
