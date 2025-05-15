package doric
package syntax

import scala.reflect.runtime.universe.TypeTag
import doric.implicitConversions.stringCname
import doric.Equalities._
import doric.types.SparkType.Primitive
import doric.types.{NumericType, SparkType}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{functions => f}
import org.scalactic.Equality

import java.sql.Date
import java.time.LocalDate
import scala.reflect.{ClassTag, classTag}

class AggregationColumns34Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  import spark.implicits._

  private def getClassName[T: ClassTag]: String =
    classTag[T].runtimeClass.getSimpleName

  describe("mode doric function") {
    def testModeFunction[T: ClassTag: SparkType: TypeTag: Equality](
        values: List[T],
        result: T
    ): Unit = {
      it(s"should work as spark mode function for [${getClassName[T]}]") {
        val df = (values.map(x => ("key1", Option(x))) :+ ("key2", None))
          .toDF("key", "value")

        df.testAggregation(
          "key",
          mode(col[T]("value")),
          f.mode(f.col("value")),
          List(Option(result), None)
        )
      }
    }

    testModeFunction(List("a", "a", "b"), result = "a")
    testModeFunction(List(true, false, false), result = false)
    testModeFunction[Byte](List(1, 1, 2), result = 1)
    testModeFunction[Short](List(1, 1, 2), result = 1)
    testModeFunction(List(1, 1, 2), result = 1)
    testModeFunction(List(1L, 1L, 2L), result = 1L)
    testModeFunction(List(2.0, 45.0, 45.0), result = 45.0)
    testModeFunction(List(1f, 1f, 2f), result = 1f)
    testModeFunction(
      List(LocalDate.parse("2025-05-01")),
      result = LocalDate.parse("2025-05-01")
    )
    testModeFunction(
      List(LocalDate.parse("2025-05-01").atStartOfDay()),
      result = LocalDate.parse("2025-05-01").atStartOfDay()
    )
    testModeFunction(
      List(Array("a", "a"), Array("b"), Array("b")),
      result = Array("b")
    )
  }

  describe("median doric function") {

    def testMedianFunction[
        T: ClassTag: NumericType: Primitive: TypeTag: Equality
    ](
        values: List[T],
        result: Double
    ): Unit = {
      it(s"should work as spark median function for [${getClassName[T]}]") {
        val df = (values.map(x => ("key1", Option(x))) :+ ("key2", None))
          .toDF("key", "value")

        df.testAggregation(
          "key",
          median(col[T]("value")),
          f.median(f.col("value")),
          List(Option(result), None)
        )
      }
    }

    testMedianFunction[Byte](List(1, 1, 2), result = 1.0)
    testMedianFunction[Short](List(1, 1, 2), result = 1.0)
    testMedianFunction(List(1, 1, 2), result = 1.0)
    testMedianFunction(List(1L, 1L, 2L), result = 1.0)
    testMedianFunction(List(2.0, 45.0, 45.0), result = 45.0)
    testMedianFunction(List(1f, 1f, 2f), result = 1.0)
  }
}
