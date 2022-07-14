package doric
package types

import org.apache.spark.unsafe.types.CalendarInterval

trait DeserializeSparkTypeSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (Spark 2.4") {

    it("should match Atomic Spark SQL types") {

      // Datetime type
      deserializeSparkType[CalendarInterval](new CalendarInterval(0, 0))
    }
  }

}