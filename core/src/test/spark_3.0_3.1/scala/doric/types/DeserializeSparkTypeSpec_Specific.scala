package doric
package types

import org.apache.spark.unsafe.types.CalendarInterval

trait DeserializeSparkTypeSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (Spark 3.0, 3.1)") {

    it("should match Atomic Spark SQL types (Spark 3.0, 3.1)") {

      // Datetime type
      deserializeSparkType[CalendarInterval](new CalendarInterval(0, 0, 0))
    }
  }

}
