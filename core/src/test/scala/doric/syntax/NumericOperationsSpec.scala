package doric
package syntax

import scala.reflect.{classTag, ClassTag}

import doric.types.{NumericType, SparkType}
import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.DataFrame

trait NumericOperationsSpec extends AnyFunSpecLike with TypedColumnTest {

  import doric.implicitConversions.stringCname

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
        test[T, T, T]((a,b) => a % b)
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
    List((1, 2f, 3L, 4.toDouble)).toDF(getName[Int](), getName[Float](), getName[Long](), getName[Double]())

  test[Int]()
  test[Float]()
  test[Long]()
  test[Double]()
}
