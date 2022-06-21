package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class ArrayColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("filterWIndex doric function") {
    import spark.implicits._

    it("should work as spark filter((Column, Column) => Column) function") {
      val df = List((Array("a", "b", "c", "d"), "b"))
        .toDF("col1", "col2")

      noException shouldBe thrownBy {
        df.select(colArrayString("col1").filterWIndex((x, i) => {
          i === 0.lit or x === colString("col2")
        }))
      }

      df.testColumns2("col1", "col2")(
        (c1, c2) =>
          colArrayString(c1).filterWIndex((x, i) => {
            i === 0.lit or x === colString(c2)
          }),
        (c1, c2) =>
          f.filter(
            f.col(c1),
            (x, i) => {
              i === 0 or x === f.col(c2)
            }
          ),
        List(Some(Array("a", "b")))
      )
    }
  }

  describe("exists doric function") {
    import spark.implicits._

    it("should work as spark exists function") {
      val df = List(Array(":a", "b", null, ":c", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns2("col1", ":")(
        (c, s) => colArrayString(c).exists(_.startsWith(s.lit)),
        (c, s) => f.exists(f.col(c), _.startsWith(s)),
        List(Some(true), Some(false), None)
      )
    }
  }

  describe("forAll doric function") {
    import spark.implicits._

    it("should work as spark forall function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).forAll(_.isNotNull),
        c => f.forall(f.col(c), _.isNotNull),
        List(Some(false), Some(true), None)
      )
    }
  }

  describe("zipWith doric function") {
    import spark.implicits._

    it("should work as spark zip_with function") {
      val df = List(
        (Array("a", "b", "c", "d"), Array("b", "a", "e")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (
            c1,
            c2
        ) => colArrayString(c1).zipWith(colArrayString(c2))(concat(_, _)),
        (c1, c2) => f.zip_with(f.col(c1), f.col(c2), f.concat(_, _)),
        List(Some(Array("ab", "ba", "ce", null)), None, None, None)
      )
    }
  }

}
