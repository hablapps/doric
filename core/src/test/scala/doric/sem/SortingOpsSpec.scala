package doric.sem

import doric.{DoricTestElements, colInt, colString}
import org.apache.spark.sql.{functions => f, Row}

class SortingOpsSpec extends DoricTestElements {
  import spark.implicits._

  describe("Sort") {
    it("sorts a dataframe with sort function on one column") {
      val df = List((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("col1", "col2")

      val res    = df.sort(colInt("col1").desc)
      val actual = List(Row(4, "d"), Row(3, "c"), Row(2, "b"), Row(1, "a"))

      res.collect().toList should contain theSameElementsInOrderAs actual
    }

    it("sorts a dataframe with sort function on multiple columns") {
      val df = List((1, "z"), (2, "n"), (3, "x"), (2, "f")).toDF("col1", "col2")

      val res    = df.sort(colInt("col1").desc, colString("col2").asc)
      val actual = List(Row(3, "x"), Row(2, "f"), Row(2, "n"), Row(1, "z"))

      res.collect().toList should contain theSameElementsInOrderAs actual
    }
  }

  describe("Sort Within Partitions") {
    it("sorts dataframe partitions with sort function on one column") {
      val df = List((2, "a"), (1, "a"), (3, "b"), (4, "b"))
        .toDF("col1", "col2")
        .repartition(2, f.col("col2"))

      val res    = df.sortWithinPartitions(colInt("col1").asc)
      val actual = List(Row(1, "a"), Row(2, "a"), Row(3, "b"), Row(4, "b"))

      res.collect().toList should contain theSameElementsInOrderAs actual
    }

    it("sorts dataframe partitions with sort function on multiple columns") {
      val df = List((1, "a"), (2, "a"), (3, "a"), (1, "b"), (2, "b"))
        .toDF("col1", "col2")
        .repartition(2, f.col("col2"))

      val res =
        df.sortWithinPartitions(colInt("col1").desc, colString("col2").asc)
      val actual =
        List(Row(3, "a"), Row(2, "a"), Row(1, "a"), Row(2, "b"), Row(1, "b"))

      res.collect().toList should contain theSameElementsInOrderAs actual
    }
  }
}
