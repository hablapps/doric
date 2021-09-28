package doric
package sem

import scala.reflect.ClassTag

import doric.types.SparkType
import doric.Equalities._
import org.scalactic.Equality
import org.scalatest.Assertion

import org.apache.spark.sql.Encoder

class CollectOpsSpec extends DoricTestElements {

  import spark.implicits._

  def test[T: SparkType: Encoder](
      l: List[T]
  )(implicit eq: Equality[List[T]]): Assertion =
    l.toDF("nums")
      .collectCols(col[T](c"nums")) should ===(l)

  describe("collectCols") {
    it("allows to collect simple columns") {
      val maybeInts = List(Option(1), None)
      test(maybeInts)

      val nullAndStrings: List[String] = List("1", null)
      test(nullAndStrings)

      val maybeStrings: List[Option[String]] = List(Some("1"), None)
      test(maybeStrings)

      val listOfStrings: List[Array[String]] =
        List(Array("a", "b"), Array.empty[String])
      test(listOfStrings)
    }

    it("allows to collect maps") {
      val x = List(Map("hola" -> "adios"))
      test(x)

      val y = List(Map("hola" -> Map("jander" -> 1)))
      test(y)
    }

    it("allows to collect custom elements") {
      implicit def listSparkType[T: SparkType: ClassTag](implicit
          arr: SparkType[List[T]]
      ): SparkType[Set[T]] {
        type OriginalSparkType = arr.OriginalSparkType
      } =
        SparkType[List[T]].customType[Set[T]](_.toSet)

      spark
        .range(1)
        .select(List(1, 2, 3, 2, 3).lit.as(c"value"))
        .collectCols(col[Set[Int]](c"value")) shouldBe List(Set(1, 2, 3))
    }
  }
}
