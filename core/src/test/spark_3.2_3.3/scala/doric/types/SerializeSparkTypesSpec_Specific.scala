package doric
package types

trait SerializeSparkTypeSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (specific version)") {

    it("should match Atomic Spark SQL types") {

      // Interval type

      // serializeSparkType[java.time.Duration](java.time.Duration.ZERO)
      // serializeSparkType[java.time.Period](java.time.Period.ZERO)

    }
  }
}
