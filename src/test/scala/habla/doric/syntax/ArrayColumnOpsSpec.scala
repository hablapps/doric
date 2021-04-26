package habla.doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

class ArrayColumnOpsSpec
    extends DoricTestElements
    with ArrayColumnOps
    with EitherValues
    with Matchers {

  import spark.implicits._

  describe("ArrayOps") {
    it("should extract a index") {
      val df = List((List(1, 2, 3), 1)).toDF("col", "something").select("col")
      df.withColumn("result", getArray[Int]("col").getIndex(1))
        .select("result")
        .as[Int]
        .head() shouldBe 2
    }

    it("should transform the elements of the array with the provided function") {
      val df = List((List(1, 2, 3), 7)).toDF("col", "something")
      df.withColumn("result", getArrayInt("col").transform(_ + getInt("something")))
        .select("result")
        .as[List[Int]]
        .head() shouldBe List(8, 9, 10)
    }

    it("should capture the error if anything in the lambda is wrong") {
      val df = List((List(1, 2, 3), 7)).toDF("col", "something")
      getArrayInt("col")
        .transform(_ + getInt("something2"))
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head
        .message shouldBe "Cannot resolve column name \"something2\" among (col, something)"

      getArrayInt("col")
        .transform(_ => getString("something"))
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head
        .message shouldBe "The column with name 'something' is of type IntegerType and it was expected to be StringType"
    }

    it("should transform with index the elements of the array with the provided function") {
      val df = List((List(10, 20, 30), 7)).toDF("col", "something").select("col")
      df.withColumn("result", getArrayInt("col").transformWithIndex(_ + _))
        .select("result")
        .as[List[Int]]
        .head() shouldBe List(10, 21, 32)
    }

    it("should capture errors in transform with index") {
      val df = List((List(10, 20, 30), "7")).toDF("col", "something")
      getArrayInt("col")
        .transformWithIndex(_ + _ + getInt("something"))
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head
        .message shouldBe "The column with name 'something' is of type StringType and it was expected to be IntegerType"

      getArrayInt("col")
        .transformWithIndex(_ + _ + getInt("something2"))
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head
        .message shouldBe "Cannot resolve column name \"something2\" among (col, something)"
    }

    it("should aggregate the elements of the array with the provided function") {
      val df = List((List(10, 20, 30), 7)).toDF("col", "something").select("col")
      df.withColumn("result", getArrayInt("col").aggregate[Int](100.lit)(_ + _))
        .select("result")
        .as[Int]
        .head() shouldBe 160
    }

    it("should capture errors in aggregate") {
      val df = List((List(10, 20, 30), "7")).toDF("col", "something")
      val errors = getArrayInt("col")
        .aggregate(getInt("something2"))(_ + _ + getInt("something"))
        .elem
        .run(df)
        .toEither
        .left
        .value

      errors.toChain.size shouldBe 2
      errors.map(_.message).toChain.toList shouldBe List(
        "Cannot resolve column name \"something2\" among (col, something)",
        "The column with name 'something' is of type StringType and it was expected to be IntegerType"
      )
    }

    it(
      "should aggregate the elements of the array with the provided function with final transform"
    ) {
      val df = List((List(10, 20, 30), 7)).toDF("col", "something")
      df.withColumn(
        "result",
        getArrayInt("col")
          .aggregate[Int, String](100.lit, _ + _, x => (x + get[Int]("something")).castTo)
      ).select("result")
        .as[String]
        .head() shouldBe "167"
    }

    it("should capture errors in aggregate with final transform") {
      val df = List((List(10, 20, 30), "7")).toDF("col", "something")
      val errors = getArrayInt("col")
        .aggregate[Int, String](
          getInt("something2"),
          _ + _ + getInt("something"),
          x => (x + getInt("something3")).castTo
        )
        .elem
        .run(df)
        .toEither
        .left
        .value

      errors.toChain.size shouldBe 3
      errors.map(_.message).toChain.toList shouldBe List(
        "Cannot resolve column name \"something2\" among (col, something)",
        "The column with name 'something' is of type StringType and it was expected to be IntegerType",
        "Cannot resolve column name \"something3\" among (col, something)"
      )
    }

    it("should filter") {
      val df = List((List(10, 20, 30), 25))
        .toDF("col", "val")
        .withColumn("result", getArrayInt("col").filter(_ < getInt("val")))
        .select("result")
        .as[List[Int]]
        .head() shouldBe List(10, 20)
    }
  }

}
