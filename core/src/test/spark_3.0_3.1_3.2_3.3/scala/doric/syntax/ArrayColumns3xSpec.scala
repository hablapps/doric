package doric
package syntax

import doric.SparkAuxFunctions.createLambda
import org.apache.spark.sql.catalyst.expressions.{ArrayFilter, ArraySort}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{Column, functions => f}

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

  describe("sort doric function") {
    import spark.implicits._

    lazy val arraySort_old: (Column, Column) => Column =
      (myCol, myExpr) => new Column(ArraySort(myCol.expr, myExpr.expr))

    it("should work as spark array_sort(expression) function") {
      val df = List(
        Array("ccc", "bb", null, null, "a", "dddd"),
        Array("z"),
        Array.empty[String],
        null
      ).toDF("col1")

      df.testColumns("col1")(
        c =>
          colArrayString(c).sortBy((l, r) =>
            coalesce(
              l.length - r.length,
              r.length,
              l.length * (-1).lit,
              0.lit
            )
          ),
        c =>
          arraySort_old(
            f.col(c),
            f.expr(
              "(l, r) ->" +
                " case when length(l) < length(r) or r is null then -1" +
                " when length(l) > length(r) then 1" +
                " else 0 end"
            )
          ),
        List(
          Some(Array("a", "bb", "ccc", "dddd", null, null)),
          Some(Array("z")),
          Some(Array.empty[String]),
          None
        )
      )
    }

    it("should order by the transformation function") {
      val df = List(
        (1 to 15).toArray,
        null
      ).toDF("col1")

      df.testColumns("col1")(
        c => colArrayInt(c).sortBy(c => c % 10.lit),
        c =>
          arraySort_old(
            f.col(c),
            f.expr(
              "(l, r) ->" +
                " case when l % 10 < r % 10 then -1" +
                " when l % 10 > r % 10 then 1" +
                " else 0 end"
            )
          ),
        List(
          Some(Array(10, 1, 11, 2, 12, 3, 13, 4, 14, 5, 15, 6, 7, 8, 9)),
          None
        )
      )
    }
  }

}
