package habla.doric
package syntax

import scala.reflect.{classTag, ClassTag}

import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.DataFrame

trait NumericOperationsSpec extends AnyFunSpecLike with TypedColumnTest {

  def df: DataFrame

  def test[T: SparkType: ClassTag, O: SparkType](
      f: DoricColumn[T] => DoricColumn[O]
  ): Unit =
    assert(
      df.withColumn("result", f(col[T](getName[T](1))))("result")
        .expr
        .dataType == dataType[O],
      "the output type is not equal to"
    )

  def test[T: NumericOperations: SparkType: ClassTag](): Unit = {

    describe(s"Numeric ${classTag[T].getClass.getSimpleName}") {

      it("+") {
        test(NumericOperations[T].+ _)
      }
      it("-") {
        test(NumericOperations[T].- _)
      }
      it("*") {
        test(NumericOperations[T].* _)
      }
      it(">") {
        test(NumericOperations[T].> _)
      }
      it(">=") {
        test(NumericOperations[T].>= _)
      }
      it("<") {
        test(NumericOperations[T].< _)
      }
      it("<=") {
        test(NumericOperations[T].<= _)
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
    List((1, 2f, 3L)).toDF(getName[Int](), getName[Float](), getName[Long]())

  test[Int]()
  test[Float]()
  test[Long]()
}
