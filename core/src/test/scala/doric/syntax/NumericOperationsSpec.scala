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

  describe("isNan doric function") {

    it("should work as spark isNaN function (Double)") {
      val df = List(Some(5.0), Some(Double.NaN), None)
        .toDF("col1")

      val res = df
        .select(colDouble("col1").isNaN)
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(false), Some(true), Some(false))
    }

    it("should work as spark isNaN function (Float)") {
      val df = List(Some(5.0f), Some(Float.NaN), None)
        .toDF("col1")

      val res = df
        .select(colFloat("col1").isNaN)
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(false), Some(true), Some(false))
    }

    it("should not work with other types") {
      """"
        |val df = List(Some(5.0), Some(Double.NaN), None)
        |        .toDF("col1")
        |
        |df.select(colDouble("col1").cast[String].isNaN)
        |""".stripMargin shouldNot compile
    }
  }

  describe("unixTimestamp doric function") {
    import spark.implicits._

    it("should work as spark unix_timestamp function") {
      val df = List(Some(123.567), Some(1.0001), None)
        .toDF("col1")

      df.testColumns("col1")(
        _ => unixTimestamp(),
        _ => f.unix_timestamp()
      )
    }
  }

  describe("random doric function") {
    import spark.implicits._

    it("should work as spark rand function") {
      val df = List(Some(123.567), None)
        .toDF("col1")

      df.validateColumnType(random())

      val res = df.select(random()).as[Double].collect().toList
      val exp = df.select(f.rand()).as[Double].collect().toList

      res.size shouldBe exp.size

      every(res) should (be >= 0.0 and be < 1.0)
      every(exp) should (be >= 0.0 and be < 1.0)
    }

    it("should work as spark rand(seed) function") {
      val df = List(Some(123.567), Some(1.0001), None)
        .toDF("col1")

      df.testColumns(1L)(
        seed => random(seed.lit),
        seed => f.rand(seed)
      )
    }
  }

  describe("randomN doric function") {
    import spark.implicits._

    it("should work as spark randn function") {
      val df = List(Some(123.567), None)
        .toDF("col1")

      df.validateColumnType(randomN())

      val res = df.select(randomN()).as[Double].collect().toList
      val exp = df.select(f.randn()).as[Double].collect().toList

      res.size shouldBe exp.size
    }

    it("should work as spark randn(seed) function") {
      val df = List(Some(123.567), Some(1.0001), None)
        .toDF("col1")

      df.testColumns(1L)(
        seed => randomN(seed.lit),
        seed => f.randn(seed)
      )
    }
  }

  describe("sparkPartitionId doric function") {
    import spark.implicits._

    it("should work as spark spark_partition_id function") {
      val df = List(Some(123.567), None)
        .toDF("col1")

      df.testColumns("")(
        _ => sparkPartitionId(),
        _ => f.spark_partition_id(),
        List(Some(0), Some(0))
      )
    }
  }

  describe("monotonicallyIncreasingId doric function") {
    import spark.implicits._

    it("should work as spark monotonically_increasing_id function") {
      val df = List(Some(123.567), None)
        .toDF("col1")

      df.testColumns("")(
        _ => monotonicallyIncreasingId(),
        _ => f.monotonically_increasing_id(),
        List(Some(0L), Some(1L))
      )
    }
  }

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
