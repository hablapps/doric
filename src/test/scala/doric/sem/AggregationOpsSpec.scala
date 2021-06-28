package doric
package sem

import doric.implicitConversions._

import org.apache.spark.sql.RelationalGroupedDataset

class AggregationOpsSpec extends DoricTestElements {

  import spark.implicits._
  describe("Aggregate") {
    val df =
      List((1, "5", 1), (3, "5", 2), (2, "3", 3))
        .toDF("num", "str", "num2")

    it("can use original spark aggregateFunctions") {
      df.groupBy("str")
        .agg(sum[Int]("num") as "sum")
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.groupBy("str")
          .agg(sum[Float]("num") as "sum")
      }
    }

    it("groupBy") {
      df.groupBy(concat(col("str"), col("str")) as "conc")
        .agg(sum[Int]("num") as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.groupBy(col[String]("str2"))
          .agg(sum[Int]("num") as "sum")
      }
    }

    it("cube") {
      df.cube(concat(col("str"), col("str")) as "conc")
        .agg(sum[Int]("num") as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.cube(col[String]("str2"))
          .agg(sum[Int]("num") as "sum")
      }
    }

    it("rollup") {
      df.rollup(concat(col("str"), col("str")) as "conc")
        .agg(sum[Int]("num") as "sum")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("sum"))

      assertThrows[DoricMultiError] {
        df.rollup(col[String]("str2"))
          .agg(sum[Int]("num") as "sum")
      }
    }

    it("pivot") {
      df.groupBy(concat(col("str"), col("str")) as "conc")
        .pivot(colInt("num2"))(List(1, 4))
        .agg(sum[Int]("num") as "sum", first[Int]("num") as "first")
        .validateColumnType(colString("conc"))
        .validateColumnType(colLong("1_sum"))
        .validateColumnType(colLong("4_sum"))
        .validateColumnType(colInt("1_first"))
        .validateColumnType(colInt("4_first"))

      assertThrows[DoricMultiError] {
        df.groupBy(concat(col("str"), col("str")) as "conc")
          .pivot(colString("num2"))(List("1", "4"))
          .agg(sum[Int]("num") as "sum")
      }
    }

    it("blabla") {

      df.select(col[String]("str").pipe(count) + sum[Int]("num")).printSchema()

      val dfg: RelationalGroupedDataset = df.groupBy(colString("str"))
      dfg.agg(sum(colInt("num"))).show

      df.groupBy("str").pivot(colInt("num"))(List(5)).count().explain(true)
      //df.select(f.sum(f.explode(f.col("arr"))).asDoric[Int] as "arr").collectCols(colInt("arr")).foreach(println)
    }
  }
}
