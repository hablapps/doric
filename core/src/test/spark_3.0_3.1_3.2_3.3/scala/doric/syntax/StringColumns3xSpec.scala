package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

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
}
