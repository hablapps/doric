package doric

import doric.types.{DateType, SparkType}
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpecLike

import scala.reflect.ClassTag

trait DateColumnTest extends AnyFunSpecLike with TypedColumnTest {

  def df: DataFrame

  def test[T: DateType: SparkType: ClassTag](tagName: String): Unit = {

    describe(s"Date type $tagName") {
      it(">") {
        test[T, T, Boolean](tagName, (a, b) => a > b)
      }
      it(">=") {
        test[T, T, Boolean](tagName, (a, b) => a >= b)
      }
      it("<") {
        test[T, T, Boolean](tagName, (a, b) => a < b)
      }
      it("<=") {
        test[T, T, Boolean](tagName, (a, b) => a <= b)
      }
    }
  }

  def test[T1: SparkType: ClassTag, T2: SparkType: ClassTag, O: SparkType](
      tagName: String,
      f: (DoricColumn[T1], DoricColumn[T2]) => DoricColumn[O]
  ): Unit =
    df.validateColumnType(
      f(
        col[T1](getName(tagName)),
        col[T2](getName(tagName))
      )
    )

  def getName(tagName: String, pos: Int = 1): String =
    s"col_${tagName}_$pos"
}
