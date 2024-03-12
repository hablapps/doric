package doric
package syntax

import doric.DoricTestElements
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{Column, Row}

class AggregationColumns32Spec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("custom aggregations") {

    it("should work") {
      spark
        .range(10000)
        .withColumn("id", 1L.lit)
        .select(
          customAgg[Long, Long, Long](
            col[Long]("id"),
            0L.lit,
            (x, y) => x + y,
            (x, y) => x + y + 100L.lit,
            identity
          ).as("result")
        )
        .collectCols(col[Long]("result"))
        .head shouldBe 10100

      spark
        .range(10)
        .withColumn("id", array(col[Long]("id")))
        .select(
          customAgg[Array[Long], Array[Long], Array[Long]](
            col("id"), // a column of arrays of Long
            array(),   // init value for the update
            // merge and make distinct in same task
            (x, y) => concatArrays(x, y).distinct,
            // merge results after the shuffle
            (x, y) => concatArrays(x, y).distinct,
            // return the array with distinct values
            identity
          ).as("result")
        )
        .collectCols(col[Array[Long]]("result"))
        .head shouldBe Array
        .range(0, 10)
        .map(_.toLong)

      spark
        .range(100)
        .select(
          customAgg[Long, Array[Long], Array[Long]](
            col("id"),
            array(),
            (a, b) => a.union(array(b)).sort(false.lit),
            _ union _,
            identity
          )
        )
    }

    it("should detect if we reference a column in a invalid method") {

      val df = spark
        .range(10000)

      val errorReference1 = customAgg[Long, Long, Long](
        col[Long]("id"),
        0.toLong.lit,
        (x, y) => x + y,
        (x, y) => x + y + 100L.lit + col("id"),
        x => x
      )

      val errors1: DoricValidated[Column] = errorReference1.elem.run(df)
      errors1.isInvalid shouldBe true
      val unwrapedErrors1 = errors1.toEither.left.value
      unwrapedErrors1.length shouldBe 1

      val errorReference2 = customAgg[Long, Long, Long](
        col[Long]("id"),
        0.toLong.lit,
        (x, y) => x + y,
        (x, y) => x + y + 100L.lit,
        x => x + col("id")
      )

      val errors2: DoricValidated[Column] = errorReference2.elem.run(df)
      errors2.isInvalid shouldBe true
      val unwrapedErrors2 = errors2.toEither.left.value
      unwrapedErrors2.length shouldBe 1

      val errorReference3 = customAgg[Long, Long, Long](
        col[Long]("id"),
        0.toLong.lit,
        (x, y) => x + y,
        (x, y) => x + y + 100L.lit + col("id"),
        x => x + col("id")
      )

      val errors3: DoricValidated[Column] = errorReference3.elem.run(df)
      errors3.isInvalid shouldBe true
      val unwrapedErrors3 = errors3.toEither.left.value
      unwrapedErrors3.length shouldBe 2
    }

    it("should accept middle values as structs") {

      val df = spark
        .range(5)

      val complexAggWithNames = customAgg[Long, Row, Double](
        col[Long]("id"),
        struct(lit(0L).as("_1"), lit(0L).as("_2")),
        (x, y) =>
          struct(
            x.getChild[Long]("_1") + y,
            x.getChild[Long]("_2") + 1L.lit
          ),
        (x, y) =>
          struct(
            x.getChild[Long]("_1") + y.getChild[Long]("_1"),
            x.getChild[Long]("_2") + y.getChild[Long]("_2")
          ),
        x => x.getChild[Long]("_1") / x.getChild[Long]("_2")
      )

      noException shouldBe thrownBy {
        df.select(complexAggWithNames)
      }

      val complexAggWithoutNames = customAgg[Long, Row, Double](
        col[Long]("id"),
        struct(lit(0L), lit(0L)),
        (x, y) =>
          struct(
            x.getChild[Long]("col1") + y,
            x.getChild[Long]("col2") + 1L.lit
          ),
        (x, y) =>
          struct(
            x.getChild[Long]("col1") + y.getChild[Long]("col1"),
            x.getChild[Long]("col2") + y.getChild[Long]("col2")
          ),
        x => x.getChild[Long]("col1") / x.getChild[Long]("col2")
      )

      noException shouldBe thrownBy {
        df.select(complexAggWithoutNames)
      }
    }
  }
}
