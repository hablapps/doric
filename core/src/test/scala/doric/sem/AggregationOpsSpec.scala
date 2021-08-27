package doric
package sem

class AggregationOpsSpec extends DoricTestElements {

  import spark.implicits._
  describe("Aggregate") {
    val df =
      List((1, "5", 1), (3, "5", 2), (2, "3", 3))
        .toDF("num", "str", "num2")

    it("can use original spark aggregateFunctions") {
      df.groupBy("str")
        .agg(colInt("num").pipe(sum(_)) as "sum")
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.groupBy("str")
          .agg(colLong("num").pipe(sum(_)) as "sum")
      }
    }

    it("groupBy") {
      df.groupBy(concat(col("str"), col("str")) as "conc")
        .agg(col[Int]("num").pipe(sum(_)) as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.groupBy(col[String]("str2"))
          .agg(col[Int]("num").pipe(sum(_)) as "sum")
      }
    }

    it("cube") {
      df.cube(concat(col("str"), col("str")) as "conc")
        .agg(col[Int]("num").pipe(sum(_)) as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.cube(col[String]("str2"))
          .agg(col[Int]("num").pipe(sum(_)) as "sum")
      }
    }

    it("rollup") {
      df.rollup(concat(col("str"), col("str")) as "conc")
        .agg(col[Int]("num").pipe(sum(_)) as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.rollup(col[String]("str2"))
          .agg(col[Int]("num").pipe(sum(_)) as "sum")
      }
    }

    it("pivot") {
      df.groupBy(concat(col("str"), col("str")) as "conc")
        .pivot(colInt("num2"))(List(1, 4))
        .agg(
          col[Int]("num").pipe(sum(_)) as "sum",
          col[Int]("num").pipe(first(_)) as "first"
        )
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("1_sum"))
        .validateColumnType(colLong("4_sum"))
        .validateColumnType(colInt("1_first"))
        .validateColumnType(colInt("4_first"))

      assertThrows[DoricMultiError] {
        df.groupBy(concat(col("str"), col("str")) as "conc")
          .pivot(colString("num2"))(List("1", "4"))
          .agg(col[Int]("num").pipe(sum(_)) as "sum")
      }
    }
  }
}
