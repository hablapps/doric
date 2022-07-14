package doric
package types

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

trait DeserializeSparkTypeSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (since Spark 3.2") {

    it("should match Atomic Spark SQL types") {

      // Datetime type
      deserializeSparkType[CalendarInterval](new CalendarInterval(0, 0, 0))

      // Interval type

      deserializeSparkType[java.time.Duration](java.time.Duration.ZERO)
      deserializeSparkType[java.time.Period](java.time.Period.ZERO)

    }
  }

}