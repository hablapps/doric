package doric
package types

import doric.types.customTypes.User
import User.{userst, userlst}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType

class SparkTypeSpec extends DoricTestElements {

  val user: User = User("name", "surname")

  def testFlow[T: SparkType: LiteralSparkType](
      element: T
  ): Unit = {
    spark
      .range(1)
      .select(element.lit.as("value"))
      .collectCols(col[T]("value")) === List(element)
  }

  it("Option should work with basic types") {
    spark
      .range(1)
      .select(lit(Option(1)) as "some", lit(None: Option[Int]) as "none")
      .collectCols(col[Option[Int]]("some"), col[Option[Int]]("none"))
      .head shouldBe (Some(1), None)
  }

  it("Row should work") {
    spark
      .range(1)
      .select(struct(lit("hi"), lit(33)) as "row")
      .collectCols(col[Row]("row"))
      .head shouldBe Row("hi", 33)
  }

  it("should work with simple custom types") {
    testFlow[User](user)
    SparkType[User].dataType === StringType
    SparkType[User].transform("name#surname") === User("name", "surname")
    SparkType[User].rowFieldTransform("name#surname") === "name#surname"
    SparkType[User].rowTransformT("name#surname") === User("name", "surname")
  }

  it("should work with custom type inside collections") {
    testFlow[List[User]](List(user))
    testFlow[Array[User]](Array(user))
  }

  it("should work for custom type with optional") {
    testFlow[Option[User]](Option(user))
  }

  it("should work all together") {
    val intToUsers = Map(1 -> List(Option(user)), 2 -> List(), 3 -> List(None))
    testFlow[Map[Int, List[Option[User]]]](intToUsers)
  }

}
