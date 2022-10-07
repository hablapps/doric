package doric
package syntax

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, functions => f}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.collection.JavaConverters._

class StringColumns3xSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("overlay doric function") {
    import spark.implicits._

    it("should work as spark overlay function") {
      val df = List(
        ("hello world", "LAMBDA WORLD", Some(7)),
        ("123456", "987654", Some(0)),
        ("hello world", "", Some(7)),
        ("hello world", null, Some(7)),
        ("hello world", "LAMBDA WORLD", None),
        (null, "LAMBDA WORLD", Some(1))
      ).toDF("col1", "col2", "col3")

      df.testColumns3("col1", "col2", "col3")(
        (
            str,
            repl,
            pos
        ) => colString(str).overlay(colString(repl), colInt(pos)),
        (str, repl, pos) => f.overlay(f.col(str), f.col(repl), f.col(pos)),
        List(
          "hello LAMBDA WORLD",
          "9876546",
          "hello world",
          null,
          null,
          null
        ).map(Option(_))
      )
    }

    it("should work as spark overlay function with length parameter") {
      val df = List(
        ("hello world", "LAMBDA WORLD", Some(7), Some(6)),
        ("123456", "987654", Some(0), Some(20)),
        ("hello world", "", Some(7), Some(20)),
        ("hello world", null, Some(7), Some(20)),
        ("hello world", "LAMBDA WORLD", None, Some(20)),
        ("hello world", "LAMBDA WORLD", Some(5), None),
        (null, "LAMBDA WORLD", Some(1), Some(20))
      ).toDF("col1", "col2", "col3", "col4")

      df.testColumns4("col1", "col2", "col3", "col4")(
        (str, repl, pos, len) =>
          colString(str).overlay(colString(repl), colInt(pos), colInt(len)),
        (str, repl, pos, len) =>
          f.overlay(f.col(str), f.col(repl), f.col(pos), f.col(len)),
        List("hello LAMBDA WORLD", "987654", "hello ", null, null, null, null)
          .map(Option(_))
      )
    }
  }

  describe("split doric function") {
    import spark.implicits._

    it("should stop splitting if limit is set") {
      val df2 = List("how are you", "hello world", "12345", null).toDF("col1")

      df2.testColumns3("col1", " ", 2)(
        (str, pattern, limit) => colString(str).split(pattern.lit, limit.lit),
        (str, pattern, limit) => f.split(f.col(str), pattern, limit),
        List(
          Array("how", "are you"),
          Array("hello", "world"),
          Array("12345"),
          null
        ).map(Option(_))
      )
    }
  }

  describe("schemaOfCsv doric function") {
    import spark.implicits._

    val df = List("column not read").toDF("col1")

    it("should work as spark schema_of_csv function") {
      df.testColumns("hello,world")(
        c => c.lit.schemaOfCsv(),
        c => f.schema_of_csv(f.lit(c)),
        List(Some("STRUCT<_c0: STRING, _c1: STRING>"))
      )
    }

    it("should work as spark schema_of_csv function using options") {
      df.testColumns2("hello|world", Map("sep" -> "|"))(
        (c, options) => c.lit.schemaOfCsv(options),
        (c, options) => f.schema_of_csv(f.lit(c), options.asJava),
        List(Some("STRUCT<_c0: STRING, _c1: STRING>"))
      )
    }
  }

  describe("schemaOfJson doric function with options") {
    import spark.implicits._

    val df = List("column not read").toDF("col1")

    it("should work as spark schema_of_json function") {
      df.testColumns2(
        "[{'col':01}]",
        Map("allowNumericLeadingZeros" -> "true")
      )(
        (c, options) => c.lit.schemaOfJson(options),
        (c, options) => f.schema_of_json(f.lit(c), options.asJava),
        List(Some("ARRAY<STRUCT<col: BIGINT>>"))
      )
    }
  }

  describe("fromCsv doric function") {
    import spark.implicits._

    val df = List("1,a,26/08/2015").toDF("col1")

    it("should work as spark from_csv(column) function") {
      df.testColumns2("col1", "a INTEGER, b STRING, date STRING")(
        (c, schema) => colString(c).fromCsv(schema.lit),
        (c, schema) =>
          f.from_csv(f.col(c), f.lit(schema), Map.empty[String, String].asJava),
        List(Some(Row(1, "a", "26/08/2015")))
      )
    }

    it("should work as spark from_csv(column) function with options") {
      df.testColumns3(
        "col1",
        "a INTEGER, b STRING, date Timestamp",
        Map("timestampFormat" -> "dd/MM/yyyy")
      )(
        (c, schema, options) => colString(c).fromCsv(schema.lit, options),
        (c, schema, options) =>
          f.from_csv(f.col(c), f.lit(schema), options.asJava),
        List(Some(Row(1, "a", Timestamp.valueOf("2015-08-26 00:00:00"))))
      )
    }

    it("should work as spark from_csv(structType) function") {
      df.testColumns2(
        "col1",
        StructType.fromDDL("a INTEGER, b STRING, date STRING")
      )(
        (c, schema) => colString(c).fromCsv2(schema),
        (c, schema) => f.from_csv(f.col(c), schema, Map.empty[String, String]),
        List(Some(Row(1, "a", "26/08/2015")))
      )
    }

    it("should work as spark from_csv(structType) function with options") {
      df.testColumns3(
        "col1",
        StructType.fromDDL("a INTEGER, b STRING, date Timestamp"),
        Map("timestampFormat" -> "dd/MM/yyyy")
      )(
        (c, schema, options) => colString(c).fromCsv2(schema, options),
        (c, schema, options) => f.from_csv(f.col(c), schema, options),
        List(Some(Row(1, "a", Timestamp.valueOf("2015-08-26 00:00:00"))))
      )
    }
  }
}
