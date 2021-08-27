package doric
package sem

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset}
import org.apache.spark.sql.doric.RelationalGroupedDatasetDoricInterface

trait AggregationOps extends RelationalGroupedDatasetDoricInterface {

  implicit class DataframeAggSyntax(df: Dataset[_]) {

    /**
      * Groups the Dataset using the specified columns, so we can run
      * aggregation on them. See
      */
    def groupBy(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkGroupBy(df.toDF(), cols: _*).returnOrThrow("groupBy")
    }

    /**
      * Create a multi-dimensional cube for the current Dataset using the
      * specified columns, so we can run aggregation on them.
      */
    def cube(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkCube(df.toDF(), cols: _*).returnOrThrow("cube")
    }

    /**
      * Create a multi-dimensional rollup for the current Dataset using the
      * specified columns, so we can run aggregation on them.
      */
    def rollup(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkRollup(df.toDF(), cols: _*).returnOrThrow("rollup")
    }
  }

  implicit class RelationalGroupedDatasetSem(rel: RelationalGroupedDataset) {

    /**
      * Compute aggregates by specifying a series of aggregate columns. Note
      * that this function by default retains the grouping columns in its
      * output. To not retain grouping columns, set
      * `spark.sql.retainGroupColumns` to false.
      */
    def agg(col: DoricColumn[_], cols: DoricColumn[_]*): DataFrame =
      sparkAgg(rel, col, cols: _*).returnOrThrow("agg")

    /**
      * Pivots a column of the current `DataFrame` and performs the specified
      * aggregation. There are two versions of pivot function: one that requires
      * the caller to specify the list of distinct values to pivot on, and one
      * that does not. The latter is more concise but less efficient, because
      * Spark needs to first compute the list of distinct values internally.
      * @param expr
      *   doric column to pivot
      * @param values
      *   the values of the column to extract
      * @tparam T
      *   The type of the column and parameters
      */
    def pivot[T](expr: DoricColumn[T])(
        values: Seq[T]
    ): RelationalGroupedDataset =
      sparkPivot(rel, expr, values).returnOrThrow("pivot")
  }

}
