package doric
package types

import doric.sem.ColumnTypeError
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{LongType, StructType}

class ProductTypesSpec extends DoricTestElements {

  // Auxiliary definitions

  import spark.implicits._

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  case class User(name: String, age: Int)
  object User {
    val schema: StructType =
      ScalaReflection.schemaFor[User].dataType.asInstanceOf[StructType]
  }

  val tuple2Schema =
    ScalaReflection.schemaFor[(String, Int)].dataType.asInstanceOf[StructType]

  val dfUsers =
    List((User("name1", 1), 1), (User("name2", 2), 2), (User("name3", 3), 3))
      .toDF("user", "delete")

  val dfTuples = List(((1, true), ""), ((2, false), ""), ((3, true), ""))
    .toDF("field", "delete")

  // Testing

  describe("Spark types for products") {

    it("should transform rows into case classes") {

      val userRow = new GenericRowWithSchema(Array("j", 1), User.schema)
      SparkType[User].transform(userRow) shouldBe User("j", 1)

      val tupleRow = new GenericRowWithSchema(Array("j", 1), tuple2Schema)
      SparkType[(String, Int)].transform(tupleRow) shouldBe ("j", 1)
    }

    it("should allow us to collect case class instances") {

      dfTuples.collectCols(col[(Int, Boolean)]("field")) shouldBe
        List((1, true), (2, false), (3, true))

      dfUsers.collectCols(col[User]("user")) shouldBe
        List(User("name1", 1), User("name2", 2), User("name3", 3))
    }

    it(
      "should return a type mismatch error if the expected and DF data types are not equal"
    ) {
      col[User]("id").elem
        .run(spark.range(1))
        .toEither
        .left
        .get
        .head shouldBe ColumnTypeError("id", User.schema, LongType)

      col[(String, Int)]("user").elem
        .run(dfUsers)
        .toEither
        .left
        .get
        .head shouldBe ColumnTypeError("user", tuple2Schema, User.schema)
    }
  }

  describe("Literal Spark Types for products") {

    it("should create columns of the right type") {

      spark.emptyDataFrame
        .select(User("name", 1).lit)
        .schema
        .fields
        .head
        .dataType shouldBe User.schema

      spark.emptyDataFrame
        .select(("name", 1).lit)
        .schema
        .fields
        .head
        .dataType shouldBe tuple2Schema
    }

    it("should work in selects, filters, ...") {

      spark
        .range(1)
        .select(User("name", 1).lit.as("user"))
        .collectCols(col[User]("user")) shouldBe
        List(User("name", 1))

      dfUsers
        .filter(col[User]("user") === User("name1", 1).lit)
        .collectCols(col[User]("user")) shouldBe
        List(User("name1", 1))
    }
  }

  describe("Dynamic field access") {

    it("should work for product types") {
      dfUsers
        .select(col[User]("user").getChild[Int]("age") as "age")
        .collectCols(col[Int]("age")) shouldBe
        List(1, 2, 3)

      dfUsers
        .select(col[User]("user").getChild[String]("name") as "name")
        .collectCols(col[String]("name")) shouldBe
        List("name1", "name2", "name3")
    }
  }
}
