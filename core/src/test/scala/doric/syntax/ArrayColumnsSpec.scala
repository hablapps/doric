package doric
package syntax

import doric.sem.{ColumnTypeError, DoricMultiError, SparkErrorWrapper}

import org.apache.spark.sql.{Row, functions => f}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class ArrayColumnsSpec extends DoricTestElements {

  import spark.implicits._

  describe("ArrayOps") {
    val result     = "result"
    val testColumn = "col"

    it("should extract a index") {
      val df = List((List(1, 2, 3), 1))
        .toDF(testColumn, "something")
        .select("col")
      df.withColumn(result, colArray[Int](testColumn).getIndex(1))
        .select(result)
        .as[Int]
        .head() shouldBe 2
    }

    it(
      "should transform the elements of the array with the provided function"
    ) {
      val df = List((List(1, 2, 3), 7)).toDF(testColumn, "something")
      df.withColumn(
        result,
        colArrayInt(testColumn).transform(_ + colInt("something"))
      ).select(result)
        .as[List[Int]]
        .head() shouldBe List(8, 9, 10)
    }

    it("should capture the error if anything in the lambda is wrong") {
      val df = List((List(1, 2, 3), 7)).toDF(testColumn, "something")

      intercept[DoricMultiError] {
        df.select(
          colArrayInt("col")
            .transform(_ + colInt("something2")),
          colArrayInt("col")
            .transform(_ => colString("something"))
        )
      } should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something2\" among (col, something)"
          )
        ),
        ColumnTypeError("something", StringType, IntegerType)
      )

      val df2 = List((List((1, "a"), (2, "b"), (3, "c")), 7))
        .toDF(testColumn, "something")

      df2
        .select(col[Array[Row]](testColumn).getIndex(0).getChild[Int]("_1"))
        .show()

      val errors = intercept[DoricMultiError] {
        df2.select(
          colArray[Row](testColumn)
            .transform(_.getChild[Int]("_3") + colInt("something2")),
          colArray[Row](testColumn)
            .transform(_.getChild[Long]("_1") + colInt("something").cast)
        )
      }
      errors.errors.toNonEmptyList.toList.foreach(println)
      errors should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something2\" among (col, something)"
          )
        ),
        ColumnTypeError("_1", LongType, IntegerType),
        ChildColumnNotFound("_3", List("_1", "_2"))
      )
    }

    it(
      "should work with complex types that mix Row and Array and return errors if needed"
    ) {

      val li = org.apache.spark.sql.functions.typedLit(4).expr
      val df3 = List((List(List((1, "a"), (2, "b"), (3, "c"))), 7))
        .toDF(testColumn, "something")

      df3.printSchema()
      df3.select(
        col[Array[Array[Row]]](testColumn)
          .transform(_.transform(_.getChild[Int]("_1")))
      )

      df3.select(
        col[Array[Array[Row]]](testColumn)
          .transform(_.getIndex(0).getChild[Int]("_1"))
      )

      intercept[DoricMultiError] {
        df3.select(
          col[Array[Array[Row]]](testColumn)
            .transform(_.transform(_.getChild[Int]("_3"))),
          col[Array[Array[Row]]](testColumn)
            .transform(_.transform(_.getChild[Long]("_1")))
        )
      } should containAllErrors(
        ChildColumnNotFound("_3", List("_1", "_2")),
        ColumnTypeError("_1", LongType, IntegerType)
      )

      val value: List[(List[(Int, String)], Long)] = List((List((1, "a")), 10L))
      val df4 = List((value, 7))
        .toDF(testColumn, "something")

      val colTransform = col[Array[Row]](testColumn)
        .transform(
          _.getChild[Array[Row]]("_1").transform(_.getChild[Int]("_1"))
        )
        .flatten as "l"
      val colTransform2 = col[Array[Row]](testColumn)
        .transform(
          _.getChild[Array[Row]]("_1")
        )
        .flatten as "l"
      df4
        .select(
          colTransform.zipWith[Row, Row]((a, b) => struct(a, b))(colTransform2)
        )
        .transform(df => { df.printSchema(); df.show(false); df })
    }

    it(
      "should transform with index the elements of the array with the provided function"
    ) {
      val df =
        List((List(10, 20, 30), 7))
          .toDF(testColumn, "something")
          .select("col")
      df.withColumn(result, colArrayInt(testColumn).transformWithIndex(_ + _))
        .select(result)
        .as[List[Int]]
        .head() shouldBe List(10, 21, 32)
    }

    it("should capture errors in transform with index") {
      val df = List((List(10, 20, 30), "7")).toDF(testColumn, "something")

      intercept[DoricMultiError] {
        df.select(
          colArrayInt(testColumn)
            .transformWithIndex(_ + _ + colInt("something")),
          colArrayInt(testColumn)
            .transformWithIndex(_ + _ + colInt("something2"))
        )
      } should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something2\" among (col, something)"
          )
        ),
        ColumnTypeError("something", IntegerType, StringType)
      )
    }

    it(
      "should aggregate the elements of the array with the provided function"
    ) {
      val df =
        List((List(10, 20, 30), 7))
          .toDF(testColumn, "something")
          .select("col")
      df.withColumn(
        result,
        colArrayInt(testColumn).aggregate[Int](100.lit)(_ + _)
      ).select(result)
        .as[Int]
        .head() shouldBe 160
    }

    it("should capture errors in aggregate") {
      val df = List((List(10, 20, 30), "7")).toDF(testColumn, "something")

      intercept[DoricMultiError] {
        df.select(
          colArrayInt(testColumn)
            .aggregate(colInt("something2"))(_ + _ + colInt("something"))
        )
      } should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something2\" among (col, something)"
          )
        ),
        ColumnTypeError("something", IntegerType, StringType)
      )
    }

    it(
      "should aggregate the elements of the array with the provided function with final transform"
    ) {
      val df = List((List(10, 20, 30), 7)).toDF(testColumn, "something")
      df.withColumn(
        result,
        colArrayInt(testColumn)
          .aggregateWT[Int, String](100.lit)(
            _ + _,
            x => (x + col[Int]("something")).cast
          )
      ).select(result)
        .as[String]
        .head() shouldBe "167"
    }

    it("should capture errors in aggregate with final transform") {
      val df = List((List(10, 20, 30), "7")).toDF(testColumn, "something")

      intercept[DoricMultiError] {
        df.select(
          colArrayInt(testColumn)
            .aggregateWT[Int, String](colInt("something2"))(
              _ + _ + colInt("something"),
              x => (x + colInt("something3")).cast
            )
        )
      } should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something2\" among (col, something)"
          )
        ),
        SparkErrorWrapper(
          new Exception(
            "Cannot resolve column name \"something3\" among (col, something)"
          )
        ),
        ColumnTypeError("something", IntegerType, StringType)
      )
    }

    it("should filter") {
      List((List(10, 20, 30), 25))
        .toDF(testColumn, "val")
        .withColumn(result, colArrayInt("col").filter(_ < c"val"))
        .select(result)
        .as[List[Int]]
        .head() shouldBe List(10, 20)
    }
  }

  describe("concatArrays doric function") {
    import spark.implicits._

    it("should work as spark concat function") {
      val df = List(
        (Array("a"), Array("b")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => concatArrays(colArrayString(c1), colArrayString(c2)),
        (c1, c2) => f.concat(f.col(c1), f.col(c2)),
        List(Some(Array("a", "b")), None, None, None)
      )
    }
  }

  describe("array doric function") {
    import spark.implicits._

    it("should work as spark array function") {
      val df = List(("a", "b"))
        .toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => array(colString(c1), colString(c2)),
        (c1, c2) => f.array(f.col(c1), f.col(c2)),
        List(Some(Array("a", "b")))
      )
    }
  }

  describe("list doric function") {
    import spark.implicits._

    it("should work as spark array function as a list") {
      val df = List(("a", "b"))
        .toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => list(colString(c1), colString(c2)),
        (c1, c2) => f.array(f.col(c1), f.col(c2)),
        List(Some(List("a", "b")))
      )
    }
  }

  describe("contains doric function") {
    import spark.implicits._

    it("should work as spark array_contains function") {
      val df = List(Array("a", "b", "c", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns2("col1", "a")(
        (c, literal) => colArrayString(c).contains(literal.lit),
        (c, literal) => f.array_contains(f.col(c), literal),
        List(Some(true), Some(false), None)
      )
    }
  }

  describe("distinct doric function") {
    import spark.implicits._

    it("should work as spark array_distinct function") {
      val df = List(Array("a", "c", "c", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).distinct,
        c => f.array_distinct(f.col(c)),
        List(Some(Array("a", "c", "d")), Some(Array("z")), None)
      )
    }
  }

  describe("except doric function") {
    import spark.implicits._

    it("should work as spark array_except function") {
      val df = List(
        (Array("a", "b", "c", "d"), Array("b", "a", "e")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => colArrayString(c1).except(col(c2)),
        (c1, c2) => f.array_except(f.col(c1), f.col(c2)),
        List(Some(Array("c", "d")), None, None, None)
      )
    }
  }

  describe("intersect doric function") {
    import spark.implicits._

    it("should work as spark array_intersect function") {
      val df = List(
        (Array("a", "b", "c", "d"), Array("b", "a", "e")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => colArrayString(c1).intersect(col(c2)),
        (c1, c2) => f.array_intersect(f.col(c1), f.col(c2)),
        List(Some(Array("a", "b")), None, None, None)
      )
    }
  }

  describe("join doric function") {
    import spark.implicits._

    it("should work as spark array_join function") {
      val df = List(Array("a", "b", "c", "d"), Array("z", null), null)
        .toDF("col1")

      df.testColumns2("col1", ",")(
        (c1, del) => colArrayString(c1).join(del.lit),
        (c1, del) => f.array_join(f.col(c1), del),
        List(Some("a,b,c,d"), Some("z"), None)
      )
    }

    it("should work as spark array_join function with null replacement") {
      val df = List(Array("a", "b", "c", "d"), Array("z", null), null)
        .toDF("col1")

      df.testColumns3("col1", ",", "-")(
        (c1, del, repl) => colArrayString(c1).join(del.lit, repl.lit),
        (c1, del, repl) => f.array_join(f.col(c1), del, repl),
        List(Some("a,b,c,d"), Some("z,-"), None)
      )
    }
  }

  describe("max doric function") {
    import spark.implicits._

    it("should work as spark array_max function") {
      val df = List(Array("a", "b", "c", "d"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).max,
        c => f.array_max(f.col(c)),
        List(Some("d"), None)
      )
    }
  }

  describe("min doric function") {
    import spark.implicits._

    it("should work as spark array_min function") {
      val df = List(Array("a", "b", "c", "d"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).min,
        c => f.array_min(f.col(c)),
        List(Some("a"), None)
      )
    }
  }

  describe("positionOf doric function") {
    import spark.implicits._

    it("should work as spark array_position function") {
      val df = List(Array("a", "b", "c", "d"), null)
        .toDF("col1")

      df.testColumns2("col1", "a")(
        (c, p) => colArrayString(c).positionOf(p.lit),
        (c, p) => f.array_position(f.col(c), p),
        List(Some(1L), None)
      )
    }
  }

  describe("remove doric function") {
    import spark.implicits._

    it("should work as spark array_remove function") {
      val df = List(Array("a", "b", "c", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns2("col1", "a")(
        (c, p) => colArrayString(c).remove(p.lit),
        (c, p) => f.array_remove(f.col(c), p),
        List(Some(Array("b", "c", "d")), Some(Array("z")), None)
      )
    }
  }

  describe("sortAscNullsLast doric function") {
    import spark.implicits._

    it("should work as spark array_sort function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).sortAscNullsLast,
        c => f.array_sort(f.col(c)),
        List(Some(Array("a", "b", "c", "d", null)), Some(Array("z")), None)
      )
    }
  }

  describe("sortAscNullsFirst doric function") {
    import spark.implicits._

    it("should work as spark array_sort function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).sortAscNullsFirst,
        c => f.sort_array(f.col(c)),
        List(Some(Array(null, "a", "b", "c", "d")), Some(Array("z")), None)
      )
    }
  }

  describe("sort doric function") {
    import spark.implicits._

    it("should work as spark array_sort(asc) function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns2("col1", false)(
        (c, ord) => colArrayString(c).sort(ord.lit),
        (c, ord) => f.sort_array(f.col(c), ord),
        List(Some(Array("d", "c", "b", "a", null)), Some(Array("z")), None)
      )
    }
  }

  describe("union doric function") {
    import spark.implicits._

    it("should work as spark array_union function") {
      val df = List(
        (Array("a", "b", "c", "d"), Array("b", "a", "e")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => colArrayString(c1).union(col(c2)),
        (c1, c2) => f.array_union(f.col(c1), f.col(c2)),
        List(Some(Array("a", "b", "c", "d", "e")), None, None, None)
      )
    }
  }

  describe("overlaps doric function") {
    import spark.implicits._

    it("should work as spark arrays_overlap function") {
      val df = List(
        (Array("a", "b", "c", "d"), Array("b", "a", "e")),
        (Array("a", "b", "c", "d"), Array("z", "w", "y")),
        (Array("a"), null),
        (null, Array("b")),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c1, c2) => colArrayString(c1).overlaps(col(c2)),
        (c1, c2) => f.arrays_overlap(f.col(c1), f.col(c2)),
        List(Some(true), Some(false), None, None, None)
      )
    }
  }

  describe("elementAt doric function") {
    import spark.implicits._

    it("should work as spark element_at function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns2("col1", 4)(
        (c, ord) => colArrayString(c).elementAt(ord.lit),
        (c, ord) => f.element_at(f.col(c), ord),
        List(Some("a"), None, None)
      )
    }
  }

  describe("explode doric function") {
    import spark.implicits._

    it("should work as spark explode function") {
      val df = List(
        ("1", Array("a", "b", "c", "d")),
        ("2", Array("e")),
        ("3", Array.empty[String]),
        ("4", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colArrayString("col").explode)
        .as[(String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.explode(f.col("col")))
        .as[(String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", "a"),
        Some("1", "b"),
        Some("1", "c"),
        Some("1", "d"),
        Some("2", "e")
      )
    }
  }

  describe("explodeOuter doric function") {
    import spark.implicits._

    it("should work as spark explode_outer function") {
      val df = List(
        ("1", Array("a", "b", "c", "d")),
        ("2", Array("e")),
        ("3", Array.empty[String]),
        ("4", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colArrayString("col").explodeOuter)
        .as[(String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.explode_outer(f.col("col")))
        .as[(String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", "a"),
        Some("1", "b"),
        Some("1", "c"),
        Some("1", "d"),
        Some("2", "e"),
        Some("3", null),
        Some("4", null)
      )
    }
  }

  describe("reverse doric function") {
    import spark.implicits._

    it("should work as spark reverse function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).reverse,
        c => f.reverse(f.col(c)),
        List(Some(Array("d", "a", null, "b", "c")), Some(Array("z")), None)
      )
    }
  }

  describe("shuffle doric function") {
    import spark.implicits._

    it("should work as spark shuffle function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      val dCols = df
        .select(colArrayString("col1").shuffle)
        .as[Array[String]]
        .collect()
        .toList
      val sCols = df
        .select(f.shuffle(f.col("col1")))
        .as[Array[String]]
        .collect()
        .toList
      val expected = List(
        Array("d", "a", null, "b", "c"),
        Array("z"),
        null
      )

      def compare(a: List[Array[String]], b: List[Array[String]]): Unit = {
        a.size shouldBe b.size
        for (i <- a.indices) {
          a(i) match {
            case l: Array[String] => l should contain theSameElementsAs b(i)
            case x if x === null  => b(i) === null
            case _                => throw new UnsupportedOperationException
          }
        }
      }

      compare(dCols, sCols)
      compare(dCols, expected)
    }
  }

  describe("size doric function") {
    import spark.implicits._

    it("should work as spark size function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colArrayString(c).size,
        c => f.size(f.col(c)),
        List(Some(5), Some(1), Some(-1))
      )
    }
  }

  describe("slice doric function") {
    import spark.implicits._

    it("should work as spark slice function") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns3("col1", 1, 2)(
        (c, s, e) => colArrayString(c).slice(s.lit, e.lit),
        (c, s, e) => f.slice(f.col(c), s, e),
        List(Some(Array("c", "b")), Some(Array("z")), None)
      )
    }

    it("should work as spark slice function when start < 0") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      df.testColumns3("col1", -1, 2)(
        (c, s, e) => colArrayString(c).slice(s.lit, e.lit),
        (c, s, e) => f.slice(f.col(c), s, e),
        List(Some(Array("d")), Some(Array("z")), None)
      )
    }

    it("should throw an exception if start = 0") {
      val df = List(Array("c", "b", null, "a", "d"), Array("z"), null)
        .toDF("col1")

      intercept[java.lang.RuntimeException](
        df.select(colArrayString("col1").slice(0.lit, 2.lit)).collect()
      )
    }
  }

}
