package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

import doric.implicitConversions.stringCname

class BinaryColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  describe("md5 doric function") {
    import spark.implicits._

    it("should work as spark md5 function with strings") {
      val df = List("this is a string", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).md5,
        c => f.md5(f.col(c)),
        List(
          Some("b37e16c620c055cf8207b999e3270e9b"),
          None
        )
      )
    }

    it("should work as spark md5 function with array of bytes") {
      val df = List(Array[Byte](1, 2, 3, 4, 5))
        .toDF("col1")

      df.testColumns("col1")(
        c => colBinary(c).md5,
        c => f.md5(f.col(c)),
        List(
          Some("7cfdd07889b3295d6a550914ab35e068")
        )
      )
    }
  }

  describe("sha1 doric function") {
    import spark.implicits._

    it("should work as spark sha1 function with strings") {
      val df = List("this is a string", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).sha1,
        c => f.sha1(f.col(c)),
        List(
          Some("517592df8fec3ad146a79a9af153db2a4d784ec5"),
          None
        )
      )
    }

    it("should work as spark sha1 function with array of bytes") {
      val df = List(Array[Byte](1, 2, 3, 4, 5))
        .toDF("col1")

      df.testColumns("col1")(
        c => colBinary(c).sha1,
        c => f.sha1(f.col(c)),
        List(
          Some("11966ab9c099f8fabefac54c08d5be2bd8c903af")
        )
      )
    }
  }

  describe("sha2 doric function") {
    import spark.implicits._

    it("should work as spark sha2 function with strings") {
      val df = List("this is a string", null)
        .toDF("col1")

      df.testColumns2("col1", 256)(
        (c, numBits) => colString(c).sha2(numBits),
        (c, numBits) => f.sha2(f.col(c), numBits),
        List(
          Some(
            "bc7e8a24e2911a5827c9b33d618531ef094937f2b3803a591c625d0ede1fffc6"
          ),
          None
        )
      )
    }

    it("should work as spark sha2 function with array of bytes") {
      val df = List(Array[Byte](1, 2, 3, 4, 5))
        .toDF("col1")

      df.testColumns2("col1", 256)(
        (c, numBits) => colBinary(c).sha2(numBits),
        (c, numBits) => f.sha2(f.col(c), numBits),
        List(
          Some(
            "74f81fe167d99b4cb41d6d0ccda82278caee9f3e2f25d5e5a3936ff3dcec60d0"
          )
        )
      )
    }

    it("should fail if numBits is not a permitted value") {
      val df = List(Array[Byte](1, 2, 3, 4, 5))
        .toDF("col1")

      val numBits = 21

      val exception = intercept[java.lang.IllegalArgumentException] {
        df.select(colBinary("col1").sha2(numBits))
      }

      exception.getMessage shouldBe
        s"requirement failed: numBits $numBits is not in the permitted values (0, 224, 256, 384, 512)"
    }
  }

  describe("crc32 doric function") {
    import spark.implicits._

    it("should work as spark crc32 function with strings") {
      val df = List("this is a string", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).crc32,
        c => f.crc32(f.col(c)),
        List(
          Some(524884034L),
          None
        )
      )
    }

    it("should work as spark crc32 function with array of bytes") {
      val df = List(Array[Byte](1, 2, 3, 4, 5))
        .toDF("col1")

      df.testColumns("col1")(
        c => colBinary(c).crc32,
        c => f.crc32(f.col(c)),
        List(
          Some(1191942644L)
        )
      )
    }
  }

}
