package doric
package functions

class FunctionsSpec extends DoricTestElements {
  import spark.implicits._
  describe("functions") {
    it("map") {
      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(
          map(colInt("k1") -> colString("v1"), colInt("k2") -> colString("v2"))
        )
    }

    it("mapFromArrays") {
      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(
          mapFromArrays[String, Int](colArrayString("key"), col("value"))
        )
    }
  }

}
