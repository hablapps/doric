package doric
package implicitConversions

class ImplicitConversionsSpec extends DoricTestElements {

  import spark.implicits._

  describe("Implicit conversions") {
    val id: String = "id"
    it(
      "should avoid creating a doric column and reference it only by the name"
    ) {
      spark
        .range(10)
        .withColumn(id, 1L.lit + id.cname)

      List("hi", "bye")
        .toDF(id)
        .withColumn(id, "¿".lit + id.cname + "?")
        .collectCols(colString(id)) shouldBe List("¿hi?", "¿bye?")
    }

    it("should allow to do implicit castings for safe casting") {
      val l: CName                  = "l"
      val i: CName                  = "i"
      val value1: DoricColumn[Long] = l[Long]
      List((1L, 1), (2L, 2))
        .toDF(l.value, i.value)
        .withColumn("result", value1 + i[Int])
    }

  }
}
