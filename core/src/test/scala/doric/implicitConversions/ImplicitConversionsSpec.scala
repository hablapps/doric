package doric
package implicitConversions

class ImplicitConversionsSpec extends DoricTestElements {

  import spark.implicits._

  describe("Implicit conversions") {
    val id: CName = "id".cname
    it(
      "should avoid creating a doric column and reference it only by the name"
    ) {
      spark
        .range(10)
        .withColumn(id, 1L.lit + id)

      List("hi", "bye")
        .toDF(id)
        .withColumn(id, "¿".lit + id + "?")
        .collectCols(id[String])
        .toList shouldBe List("¿hi?", "¿bye?")
    }

    it("should allow to do implicit castings for safe casting") {
      val l                         = "l".cname
      val i                         = "i".cname
      val value1: DoricColumn[Long] = l[Long]
      List((1L, 1), (2L, 2))
        .toDF(l, i)
        .withColumn("result".cname, value1 + i[Int])
    }

  }
}
