package doric
package sem

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
      .collectCols(col[T]("nums")) should ===(l)

  def test[T1: SparkType, T2: SparkType](
      l: List[(T1, T2)]
  )(implicit
      eq: Equality[List[(T1, T2)]],
      encoder: Encoder[(T1, T2)]
  ): Assertion = {
    l.toDF("col1", "col2")
      .collectCols(col[T1]("col1"), col[T2]("col2")) should ===(l)
  }

  def test[T1: SparkType, T2: SparkType, T3: SparkType](
      l: List[(T1, T2, T3)]
  )(implicit
      eq: Equality[List[(T1, T2, T3)]],
      encoder: Encoder[(T1, T2, T3)]
  ): Assertion = {
    l.toDF("col1", "col2", "col3")
      .collectCols(
        col[T1]("col1"),
        col[T2]("col2"),
        col[T3]("col3")
      ) should ===(l)
  }

  def test[T1: SparkType, T2: SparkType, T3: SparkType, T4: SparkType](
      l: List[(T1, T2, T3, T4)]
  )(implicit
      eq: Equality[List[(T1, T2, T3, T4)]],
      encoder: Encoder[(T1, T2, T3, T4)]
  ): Assertion = {
    l.toDF("col1", "col2", "col3", "col4")
      .collectCols(
        col[T1]("col1"),
        col[T2]("col2"),
        col[T3]("col3"),
        col[T4]("col4")
      ) should ===(l)
  }

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

    it("allows to collect more than one column") {
      test(List(("str", 1)))
      test(List(("str", 1, "str2")))
      test(List(("str", 1, "str2", 67L)))
    }

    it("allows to collect maps") {
      val x = List(Map("hola" -> "adios"))
      test(x)

      val y = List(Map("hola" -> Map("jander" -> 1)))
      test(y)
    }

    it("allows to collect custom elements") {
      implicit def listSparkType[T: SparkType](implicit
          st: SparkType[List[T]]
      ): SparkType.Custom[Set[T], st.OriginalSparkType] =
        SparkType[List[T]].customType[Set[T]](_.toSet)

      spark
        .range(1)
        .select(List(1, 2, 3, 2, 3).lit.as("value"))
        .collectCols(col[Set[Int]]("value")) shouldBe List(Set(1, 2, 3))
    }
  }
}
