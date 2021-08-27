package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.functions.{col => sparkCol}

class AsSpec extends DoricTestElements with EitherValues with Matchers {

  describe("as method") {
    import spark.implicits._

    val df = List((1, "1")).toDF("int", "str")

    it("should return a doricColum if it passes all validations") {
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
      val errors         = originalColumn.elem.run(df).toEither.left.value
      errors.length shouldBe 1
      errors.head.message.take(
        57
      ) shouldBe "cannot resolve '`error`' given input columns: [int, str];"
      errors.head.location.fileName.value shouldBe "AsSpec.scala"
    }

    it("should return a SparkError if the column doesn't match the type") {
      val originalColumn = sparkCol("int").asDoric[String]
      val errors         = originalColumn.elem.run(df).toEither.left.value
      errors.length shouldBe 1
      errors.head.message shouldBe "The column with name 'int' is of type IntegerType and it was expected to be StringType"
      errors.head.location.fileName.value shouldBe "AsSpec.scala"
    }
  }

}
