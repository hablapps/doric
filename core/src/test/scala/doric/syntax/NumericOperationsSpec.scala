package doric
package syntax

import doric.types.{NumericType, SparkType}
import doric.types.SparkType.Primitive
import org.apache.spark.sql.catalyst.expressions.{BitwiseNot, ShiftLeft, ShiftRight, ShiftRightUnsigned}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => f}
import org.scalatest.funspec.AnyFunSpecLike

import scala.reflect.ClassTag

trait NumericOperationsSpec
    extends AnyFunSpecLike
    with TypedColumnTest
    with NumericUtilsSpec {

  def df: DataFrame

  import scala.reflect.runtime.universe._
  def test[T: NumericType: Primitive: ClassTag: TypeTag]()(implicit
      spark: SparkSession,
      fun: FromInt[T]
  ): Unit = {

    val numTypeStr = getClassName[T]

    describe(s"Numeric $numTypeStr") {

      it("+") {
        validate[T, T, T](df, (a, b) => a + b)
      }
      it("-") {
        validate[T, T, T](df, (a, b) => a - b)
      }
      it("*") {
        validate[T, T, T](df, (a, b) => a * b)
      }
      it("/") {
        validate[T, T, Double](df, (a, b) => a / b)
      }
      it("%") {
        validate[T, T, T](df, (a, b) => a % b)
      }
      it(">") {
        validate[T, T, Boolean](df, (a, b) => a > b)
      }
      it(">=") {
        validate[T, T, Boolean](df, (a, b) => a >= b)
      }
      it("<") {
        validate[T, T, Boolean](df, (a, b) => a < b)
      }
      it("<=") {
        validate[T, T, Boolean](df, (a, b) => a <= b)
      }

      it(s"abs function $numTypeStr") {
        testDoricSpark[T, T](
          List(Some(-1), Some(0), Some(1), None),
          List(Some(1), Some(0), Some(1), None),
          _.abs,
          f.abs
        )
      }

      it(s"acos function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(0), Some(1), None),
          List(Some(3.14159), Some(1.57080), Some(0.0), None),
          _.acos,
          f.acos
        )
      }

      it(s"asin function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-1.57079), Some(1.57080), None, None),
          _.asin,
          f.asin
        )
      }

      it(s"atan function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.78538), Some(0.78540), Some(1.10715), None),
          _.atan,
          f.atan
        )
      }

      it(s"atan2 function $numTypeStr") {
        testDoricSpark2[T, T, Double](
          List(
            (Some(-1), Some(-1)),
            (Some(1), Some(1)),
            (Some(2), Some(0)),
            (Some(2), None),
            (None, Some(0)),
            (None, None)
          ),
          List(Some(-2.35619), Some(0.78540), Some(1.57080), None, None, None),
          _.atan2(_),
          f.atan2
        )
      }

      it(s"bin function $numTypeStr") {
        testDoricSpark[T, String](
          List(Some(0), Some(1), Some(2), None),
          List(Some("0"), Some("1"), Some("10"), None),
          _.bin,
          f.bin
        )
      }

      it(s"cbrt function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-1.0), Some(1.0), Some(1.25992), None),
          _.cbrt,
          f.cbrt
        )
      }

      it(s"cos function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(0.54030), Some(0.54030), Some(-0.41615), None),
          _.cos,
          f.cos
        )
      }

      it(s"cosh function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(1.54308), Some(1.54308), Some(3.76220), None),
          _.cosh,
          f.cosh
        )
      }

      it(s"degrees function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-57.29578), Some(57.29578), Some(114.59156), None),
          _.degrees,
          f.degrees
        )
      }

      it(s"exp function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(0.36788), Some(2.71828), Some(7.38906), None),
          _.exp,
          f.exp
        )
      }

      it(s"expm1 function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.63212), Some(1.71828), Some(6.38906), None),
          _.expm1,
          f.expm1
        )
      }

      it(s"factorial function $numTypeStr") {
        testDoricSpark[T, Long](
          List(Some(-1), Some(1), Some(10), None),
          List(None, Some(1L), Some(3628800L), None),
          _.factorial,
          f.factorial
        )
      }

      it(s"hypot function $numTypeStr") {
        testDoricSpark2[T, T, Double](
          List(
            (Some(-1), Some(1)),
            (Some(1), Some(1)),
            (Some(2), Some(1)),
            (Some(2), None),
            (None, Some(1)),
            (None, None)
          ),
          List(Some(1.41421), Some(1.41421), Some(2.23607), None, None, None),
          _.hypot(_),
          f.hypot
        )
      }

      it(s"log function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(0.0), Some(0.69315), None),
          _.log,
          f.log
        )
      }

      it(s"log10 function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(0.0), Some(0.30103), None),
          _.log10,
          f.log10
        )
      }

      it(s"log1p function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(0.69314), Some(1.09861), None),
          _.log1p,
          f.log1p
        )
      }

      it(s"log2 function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(0), Some(1), None),
          _.log2,
          f.log2
        )
      }

      it(s"pow function $numTypeStr") {
        testDoricSpark2[T, T, Double](
          List(
            (Some(-1), Some(2)),
            (Some(1), Some(5)),
            (Some(2), Some(2)),
            (Some(2), None),
            (None, Some(2)),
            (None, None)
          ),
          List(Some(1), Some(1), Some(4), None, None, None),
          _.pow(_),
          f.pow
        )
      }

      it(s"pMod function $numTypeStr") {
        testDoricSpark2[T, T, T](
          List(
            (Some(-10), Some(3)),
            (Some(10), Some(3)),
            (Some(2), Some(2)),
            (Some(2), None),
            (None, Some(2)),
            (None, None)
          ),
          List(Some(2), Some(1), Some(0), None, None, None),
          _.pMod(_),
          f.pmod
        )
      }

      it(s"radians function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.01745), Some(0.01745), Some(0.03491), None),
          _.radians,
          f.radians
        )
      }

      it(s"rint function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-1.0), Some(1.0), Some(2.0), None),
          _.rint,
          f.rint
        )
      }

      it(s"signum function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(2), None),
          List(Some(-1.0), Some(1.0), None),
          _.signum,
          f.signum
        )
      }

      it(s"sin function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.84147), Some(0.84147), Some(0.90930), None),
          _.sin,
          f.sin
        )
      }

      it(s"sinh function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-1.17520), Some(1.17520), Some(3.62686), None),
          _.sinh,
          f.sinh
        )
      }

      it(s"sqrt function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(None, Some(1.0), Some(1.41421), None),
          _.sqrt,
          f.sqrt
        )
      }

      it(s"tan function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-1.55741), Some(1.55741), Some(-2.18504), None),
          _.tan,
          f.tan
        )
      }

      it(s"tanh function $numTypeStr") {
        testDoricSpark[T, Double](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(-0.76159), Some(0.76159), Some(0.96403), None),
          _.tanh,
          f.tanh
        )
      }

      it(s"negate function $numTypeStr") {
        testDoricSpark[T, T](
          List(Some(-1), Some(1), Some(2), None),
          List(Some(1), Some(-1), Some(2), None),
          _.negate,
          f.negate
        )
      }
    }
  }

  def testIntegrals[T: IntegralType: ClassTag: TypeTag]()(implicit
      spark: SparkSession,
      sparkTypeT: SparkType[T],
      fun: FromInt[T]
  ): Unit = {
    val numTypeStr   = getClassName[T]
    implicit val AST = SparkType.fromArray[T]

    describe(s"Integral num $numTypeStr") {
      it(s"sequence function $numTypeStr") {
        val to = 5
        testDoricSpark[T, Array[T]](
          List(Some(-1), None),
          List(Some(Array(-1, 0, 1, 2, 3, 4, 5)), None),
          _.sequence(to.lit),
          f.sequence(_, f.lit(to))
        )
      }

      it(s"sequenceT function $numTypeStr") {
        val to = 5
        testDoricSpark[T, Array[T]](
          List(Some(-1), None),
          List(Some(Array(-1, 0, 1, 2, 3, 4, 5)), None),
          _.sequenceT(to.lit),
          f.sequence(_, f.lit(to))
        )
      }

      it(s"sequence function with param $numTypeStr") {
        val to   = 6
        val step = 2
        testDoricSpark[T, Array[T]](
          List(Some(-1), None),
          List(Some(Array(-1, 1, 3, 5)), None),
          _.sequence(to.lit, step.lit),
          f.sequence(_, f.lit(to), f.lit(step))
        )
      }

      it(s"sequenceT function with param $numTypeStr") {
        val to   = 6
        val step = 2
        testDoricSpark[T, Array[T]](
          List(Some(-1), None),
          List(Some(Array(-1, 1, 3, 5)), None),
          _.sequenceT(to.lit, step.lit),
          f.sequence(_, f.lit(to), f.lit(step))
        )
      }

      it(s"hex function $numTypeStr") {
        testDoricSpark[T, String](
          List(Some(0), Some(1), Some(10), None),
          List(Some("0"), Some("1"), Some("A"), None),
          _.hex,
          f.hex
        )
      }

      it(s"shiftLeft function $numTypeStr") {
        // Aux function as it is deprecated since 3.2, otherwise specs would get complicated
        val shiftLeftBefore32: (Column, Column) => Column =
          (col, int) => new Column(ShiftLeft(col.expr, int.expr))
        val numBits = 2
        testDoricSpark[T, T](
          List(Some(0), Some(1), Some(10), None),
          List(Some(0), Some(4), Some(40), None),
          _.shiftLeft(numBits.lit),
          shiftLeftBefore32(_, f.lit(numBits))
        )
      }

      it(s"shiftRight function $numTypeStr") {
        // Aux function as it is deprecated since 3.2, otherwise specs would get complicated
        val shiftRightBefore32: (Column, Column) => Column =
          (col, int) => new Column(ShiftRight(col.expr, int.expr))
        val numBits = 2
        testDoricSpark[T, T](
          List(Some(0), Some(4), Some(-10), None),
          List(Some(0), Some(1), Some(-3), None),
          _.shiftRight(numBits.lit),
          shiftRightBefore32(_, f.lit(numBits))
        )
      }

      it(s"shiftRightUnsigned function $numTypeStr") {
        // Aux function as it is deprecated since 3.2, otherwise specs would get complicated
        val shiftRightUnsignedBefore32: (Column, Column) => Column =
          (col, int) => new Column(ShiftRightUnsigned(col.expr, int.expr))
        val numBits = 2
        testDoricSpark[T, T](
          List(Some(0), Some(4), Some(20), None),
          List(Some(0), Some(1), Some(5), None),
          _.shiftRightUnsigned(numBits.lit),
          shiftRightUnsignedBefore32(_, f.lit(numBits))
        )
      }

      it(s"bitwiseNot function $numTypeStr") {
        // Aux function as it is deprecated since 3.2, otherwise specs would get complicated
        val bitwiseNotBefore32: Column => Column =
          col => new Column(BitwiseNot(col.expr))
        val numBits = 2
        testDoricSpark[T, T](
          List(Some(0), Some(4), Some(20), None),
          List(Some(0), Some(1), Some(5), None),
          _.bitwiseNot,
          bitwiseNotBefore32
        )
      }
    }
  }

  def testDecimals[T: NumWithDecimalsType: Primitive: ClassTag: TypeTag]()(
      implicit
      spark: SparkSession,
      fun: FromFloat[T]
  ): Unit = {
    val numTypeStr = getClassName[T]

    describe(s"Num with Decimals $numTypeStr") {

      it(s"bRound function $numTypeStr") {
        testDoricSparkDecimals[T, T](
          List(Some(-0.2f), Some(0.8f), Some(0.0f), None),
          List(Some(0.0f), Some(1.0f), Some(0.0f), None),
          _.bRound,
          f.bround
        )
      }

      it(s"bRound function with param $numTypeStr") {
        val scale = 2
        testDoricSparkDecimals[T, T](
          List(Some(-0.2567f), Some(0.811f), Some(0.0f), None),
          List(Some(-0.26f), Some(0.81f), Some(0.0f), None),
          _.bRound(scale.lit),
          f.bround(_, scale)
        )
      }

      it(s"bRound function with param=0 $numTypeStr") {
        val scale = 0
        testDoricSparkDecimals[T, T](
          List(Some(-0.2567f), Some(0.811f), Some(0.0f), None),
          List(Some(0.0f), Some(1.0f), Some(0.0f), None),
          _.bRound(scale.lit),
          f.bround(_, scale)
        )
      }

      it(s"ceil function $numTypeStr") {
        testDoricSparkDecimals[T, Long](
          List(Some(-1.876458f), Some(0.12354f), Some(1.0f), None),
          List(Some(-1L), Some(1L), Some(1L), None),
          _.ceil,
          f.ceil
        )
      }

      it(s"floor function $numTypeStr") {
        testDoricSparkDecimals[T, Long](
          List(Some(-1.876458f), Some(0.12354f), Some(1.0f), None),
          List(Some(-2L), Some(0L), Some(1L), None),
          _.floor,
          f.floor
        )
      }

      it(s"round function $numTypeStr") {
        testDoricSparkDecimals[T, T](
          List(Some(-1.4f), Some(0.7f), Some(1.0f), None),
          List(Some(-1.0f), Some(1.0f), Some(1.0f), None),
          _.round,
          f.round
        )
      }

      it(s"round function with param $numTypeStr") {
        testDoricSparkDecimals[T, T](
          List(Some(-1.466f), Some(0.7111f), Some(1.0f), None),
          List(Some(-1.47f), Some(0.71f), Some(1.0f), None),
          _.round(2.lit),
          f.round(_, 2)
        )
      }

      it(s"naNvl function with param $numTypeStr") {
        testDoricSparkDecimals2[T, T, T](
          List(
            (Some(-1.466f), Some(-2.0f)),
            (Some(0f), Some(0.7111f)),
            (Some(1f / 0f), Some(1.0f)),
            (None, Some(1.0f)),
            (Some(1.0f), None),
            (None, None)
          ),
          List(
            Some(-1.466f),
            Some(0f),
            Some(1.0f),
            Some(1.0f),
            Some(1.0f),
            None
          ),
          _.naNvl(_),
          f.nanvl
        )
      }
    }
  }
}

class NumericSpec extends NumericOperationsSpec with SparkSessionTestWrapper {

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

  testIntegrals[Int]()
  testIntegrals[Long]()

  testDecimals[Float]()
  testDecimals[Double]()

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

    it("should not work with other numeric types") {
      """"
        |val df = List(Some(1), Some(2), None)
        |        .toDF("col1")
        |
        |df.select(colInt("col1").isNaN)
        |""".stripMargin shouldNot compile
    }

    it("should not work with other types") {
      """"
        |val df = List(Some("hola"), Some("doric"), None)
        |        .toDF("col1")
        |
        |df.select(colString("col1").isNaN)
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

    if (spark.version.take(3) > "3.0") {
      it("should fail if wrong pattern is given") {
        val df = List(Some(123L), Some(1L), None)
          .toDF("col1")
        intercept[java.lang.IllegalArgumentException](
          df.select(colLong("col1").fromUnixTime("wrong pattern".lit))
            .collect()
        )
      }
    }
  }
}
