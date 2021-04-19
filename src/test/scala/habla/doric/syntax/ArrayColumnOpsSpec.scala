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
        .getMessage shouldBe "Cannot resolve column name \"something2\" among (col, something)"

      getArrayInt("col")
        .transform(_ => getString("something"))
        .elem
        .run(df)
        .toEither
        .left
        .value
        .head
        .getMessage shouldBe "The column with name 'something' is of type IntegerType and it was expected to be StringType"
    }

    it("should transform with index the elements of the array with the provided function") {
      val df = List((List(10, 20, 30), 7)).toDF("col", "something").select("col")
      df.withColumn("result", getArrayInt("col").transformWithIndex(_ + _))
        .select("result")
        .as[List[Int]]
        .head() shouldBe List(10, 21, 32)
    }
  }

}
