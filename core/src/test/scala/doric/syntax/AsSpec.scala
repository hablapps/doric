package doric
package syntax

import doric.sem.{ColumnTypeError, DoricMultiError, SparkErrorWrapper}
import org.apache.spark.sql.functions.{col => sparkCol}
import org.apache.spark.sql.types.{IntegerType, StringType}

class AsSpec extends DoricTestElements {

  describe("as method") {
    import spark.implicits._

    val df = List((1, "1")).toDF("int", "str")

    it("should return a doricColumn if it passes all validations") {
      val originalColumn = sparkCol("int")
      originalColumn
        .asDoric[Int]
        .elem
        .run(df)
        .toEither
        .value shouldBe originalColumn
    }

    it("should return a SparkError if the column doesn't exist") {
      val originalColumn = sparkCol("error").asDoric[Int]

      intercept[DoricMultiError] {
        df.select(originalColumn)
      } should containAllErrors(
        SparkErrorWrapper(
          new Exception(
            "cannot resolve 'error' given input columns: [int, str]"
          )
        )
      )
    }

    it("should return a SparkError if the column doesn't match the type") {
      val originalColumn = sparkCol("int").asDoric[String]

      intercept[DoricMultiError] {
        df.select(originalColumn)
      } should containAllErrors(
        ColumnTypeError("int", StringType, IntegerType)
      )
    }
  }

}
