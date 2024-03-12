package doric
package syntax

import doric.testUtilities.data.User
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{Column, Row, functions => f}
import org.apache.spark.sql.catalyst.expressions.ArraySort

class ArrayColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  import spark.implicits._

  describe("filterWIndex doric function") {

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
  describe("sort doric function") {

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

    it("should not order by columns if it is not a struct column") {
      """colArray[Int]("test").sortBy(CName("name"), CName("surname"), CName("age"))""" shouldNot compile
    }

    it("should order structs by columns (default order CName)") {

      val (c, n1, n2, n3) = ("col1", "name", "surname", "age")

      val df = Seq(
        Seq(
          User("James", "Bond", 30),
          User("James", "Cameron", 68),
          User("King", "Kong", 30),
          User(null, "Snow", 21),
          User("Indiana", "Jones", 30),
          User("Batman", null, 35),
          User(null, "Snow", 17),
          User("Batman", null, 30),
          User("Smeagol", null, 500)
        ),
        List.empty,
        null
      ).toDF("col1")

      val doricValue = df
        .select(colArray[Row](c).sortBy(CName(n1), CName(n2), CName(n3)))
        .as[List[User]]
        .collect()
        .toList
      val sparkValue = df
        .select(
          arraySort_old(
            f.col(c),
            f.expr({
              val caseWhen: (String, String) => String = (colname, otherwise) =>
                s"case when l['$colname'] < r['$colname'] or (l['$colname'] is not null and r['$colname'] is null) then -1" +
                  s" when l['$colname'] > r['$colname'] or (l['$colname'] is null and r['$colname'] is not null) then 1" +
                  s" else $otherwise end"

              "(l, r) -> " + caseWhen(n1, caseWhen(n2, caseWhen(n3, "0")))
            })
          )
        )
        .as[List[User]]
        .collect()
        .toList
      val expected = List(
        List(
          User("Batman", null, 30),
          User("Batman", null, 35),
          User("Indiana", "Jones", 30),
          User("James", "Bond", 30),
          User("James", "Cameron", 68),
          User("King", "Kong", 30),
          User("Smeagol", null, 500),
          User(null, "Snow", 17),
          User(null, "Snow", 21)
        ),
        List.empty,
        null
      )

      assert(
        doricValue == sparkValue && doricValue == expected,
        s"\nDoric or spark result is not as expected\n" +
          s"Doric   : $doricValue\n" +
          s"Spark   : $sparkValue}\n" +
          s"Expected: $expected"
      )
    }

    it("should order structs by columns using ordered CName") {

      val (c, n1, n2, n3) = ("col1", "name", "surname", "age")

      val df = Seq(
        Seq(
          User("James", "Bond", 30),
          User("James", "Cameron", 68),
          User("King", "Kong", 30),
          User(null, "Snow", 21),
          User("Indiana", "Jones", 30),
          User("Batman", null, 35),
          User(null, "Snow", 17),
          User("Batman", null, 30),
          User("Smeagol", null, 500)
        ),
        Seq.empty,
        null
      ).toDF("col1")

      val doricValue = df
        .select(
          colArray[Row](c).sortBy(CName(n1), CNameOrd(n2), CNameOrd(n3, Desc))
        )
        .as[List[User]]
        .collect()
        .toList
      val sparkValue = df
        .select(
          arraySort_old(
            f.col(c),
            f.expr({
              val caseWhenAsc
                  : (String, String) => String = (colname, otherwise) =>
                s"case when l['$colname'] < r['$colname'] or (l['$colname'] is not null and r['$colname'] is null) then -1" +
                  s" when l['$colname'] > r['$colname'] or (l['$colname'] is null and r['$colname'] is not null) then 1" +
                  s" else $otherwise end"
              val caseWhenDesc
                  : (String, String) => String = (colname, otherwise) =>
                s"case when l['$colname'] > r['$colname'] or (l['$colname'] is not null and r['$colname'] is null) then -1" +
                  s" when l['$colname'] < r['$colname'] or (l['$colname'] is null and r['$colname'] is not null) then 1" +
                  s" else $otherwise end"

              "(l, r) -> " + caseWhenAsc(
                n1,
                caseWhenAsc(n2, caseWhenDesc(n3, "0"))
              )
            })
          )
        )
        .as[List[User]]
        .collect()
        .toList
      val expected = List(
        List(
          User("Batman", null, 35),
          User("Batman", null, 30),
          User("Indiana", "Jones", 30),
          User("James", "Bond", 30),
          User("James", "Cameron", 68),
          User("King", "Kong", 30),
          User("Smeagol", null, 500),
          User(null, "Snow", 21),
          User(null, "Snow", 17)
        ),
        Seq.empty,
        null
      )

      assert(
        doricValue == sparkValue && doricValue == expected,
        s"\nDoric or spark result is not as expected\n" +
          s"Doric   : $doricValue\n" +
          s"Spark   : $sparkValue}\n" +
          s"Expected: $expected"
      )
    }

    lazy val input = Seq(
      User("B", "", 0),
      User(null, "", 0),
      User("", "", 0),
      User("A", "", 0)
    )

    def testOrder(order: Order, expected: Seq[String]): Unit = {

      it(s"should order ${order.getClass.getSimpleName}") {
        val df = Seq(input).toDF("value")

        df.select(colArray[Row]("value").sortBy(CNameOrd("name", order)))
          .as[List[User]]
          .collect()
          .toList
          .head
          .map(_.name) shouldBe expected
      }
    }

    testOrder(Asc, Seq("", "A", "B", null))
    testOrder(AscNullsLast, Seq("", "A", "B", null))
    testOrder(AscNullsFirst, Seq(null, "", "A", "B"))
    testOrder(Desc, Seq(null, "B", "A", ""))
    testOrder(DescNullsLast, Seq("B", "A", "", null))
    testOrder(DescNullsFirst, Seq(null, "B", "A", ""))
  }

}
