package doric
package types

trait PrimitiveTypesSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (since Spark 3.2)") {

    it("should match Atomic Spark SQL types") {

      // Interval type

      testDataType[java.time.Duration]
      testDataType[java.time.Period]
    }
  }

}
