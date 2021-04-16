package habla.doric
package functions

class FunctionsSpec extends DoricTestElements {
  import spark.implicits._
  describe("functions") {
    it("map") {
      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(map(getInt("k1") -> getString("v1"), getInt("k2") -> getString("v2")))
    }

    it("mapFromArrays") {
      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(mapFromArrays[String, Int](getArrayString("key"), get("value")))
    }
  }

}
