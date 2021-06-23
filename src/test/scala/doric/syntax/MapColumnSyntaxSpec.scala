package doric
package syntax

import doric.implicitConversions._

class MapColumnSyntaxSpec extends DoricTestElements with MapColumnSyntax {

  import spark.implicits._

  private val df = List((Map("hola" -> 15), "hola"))
    .toDF("col", "validkey")

  describe("Map column") {

    it("map") {
      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(
          map(colInt("k1") -> colString("v1"), colInt("k2") -> colString("v2"))
        )

      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(
          map[Int, String]((col("k1"), col("v1")), (col("k2"), col("v2")))
        )
    }

    it("mapFromArrays") {
      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(
          mapFromArrays[String, Int](colArrayString("key"), col("value"))
        )

      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(
          mapFromArrays[String, Int](col("key"), col("value"))
        )
    }

    it("can get values from the keys") {
      df.validateColumnType(
        col[Map[String, Int]]("col").get(colString("validkey"))
      )
      df.validateColumnType(col[Map[String, Int]]("col").get("validkey"))
    }

    it("can get the keys array") {
      df.validateColumnType(col[Map[String, Int]]("col").keys)
    }

    it("can get the values array") {
      df.validateColumnType(col[Map[String, Int]]("col").values)
    }
  }

}
