package doric

import org.apache.spark.sql.Row
object AggExample {
  val customMean: DoricColumn[Double] = customAgg[Long, Row, Double](
    col[Long]("id"),          // The column to work on
    struct(lit(0L), lit(0L)), // setting the sum and the count to 0
    (x, y) =>
      struct(
        x.getChild[Long]("col1") + y, // adding the value to the sum
        x.getChild[Long](
          "col2"
        ) + 1L.lit // increasing in 1 the count of elemnts
      ),
    (x, y) =>
      struct(
        x.getChild[Long]("col1") + y.getChild[Long](
          "col1"
        ), // obtaining the total sum of all
        x.getChild[Long]("col2") + y.getChild[Long](
          "col2"
        ) // obtaining the total count of all
      ),
    x =>
      x.getChild[Long]("col1").cast[Double] / x
        .getChild[Long]("col2")
        .cast[Double] // the total sum divided by the count
  )
}
