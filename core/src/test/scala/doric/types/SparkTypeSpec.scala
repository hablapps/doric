package doric
package types

import doric.types.customTypes.User

import org.apache.spark.sql.Row

class SparkTypeSpec extends DoricTestElements {

  val user: User = User("name", "surname")

  def testFlow[T: SparkType: LiteralSparkType](
      element: T
  ): Unit = {
    spark
      .range(1)
      .select(element.lit.as(c"value"))
      .collectCols(col[T](c"value")) shouldBe List(element)
  }

  it("Option should work with basic types") {
    spark
      .range(1)
      .select(lit(Option(1)) as c"some", lit(None: Option[Int]) as c"none")
      .collectCols(col[Option[Int]](c"some"), col[Option[Int]](c"none"))
      .head shouldBe (Some(1), None)
  }

  it("Row should work") {
    spark
      .range(1)
      .select(struct(lit("hi"), lit(33)) as c"row")
      .collectCols(col[Row](c"row"))
      .head shouldBe Row("hi", 33)
  }

  it("should work with simple custom types") {
    testFlow[User](user)
  }

  it("should work with custom type inside collections") {
    testFlow[List[User]](List(user))

    val intToUsers = Map(1 -> List(Option(user)), 2 -> List(), 3 -> List(None))

    testFlow[Map[Int, List[Option[User]]]](intToUsers)
  }

  it("should work for custom type with optional") {
    testFlow[Option[User]](Option(user))
  }

}
