package doric
package sem

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset}
import org.apache.spark.sql.doric.RelationalGroupedDatasetDoricInterface

private[sem] trait AggregationOps
    extends RelationalGroupedDatasetDoricInterface {

  implicit class DataframeAggSyntax(df: Dataset[_]) {

    /**
      * Groups the Dataset using the specified columns, so we can run
      * aggregation on them.
      *
      * @group Group Dataframe operation
      * @see [[org.apache.spark.sql.Dataset.groupBy(cols:* org.apache.spark.sql.Dataset.groupBy]]
      */
    def groupBy(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkGroupBy(df.toDF(), cols: _*).returnOrThrow("groupBy")
    }

    /**
      * Groups the Dataset using the specified column names, so we can run
      * aggregation on them.
      *
      * @group Group Dataframe operation
      * @see [[org.apache.spark.sql.Dataset.groupBy(col1:* org.apache.spark.sql.Dataset.groupBy]]
      */
    @inline def groupByCName(
        col: CName,
        cols: CName*
    ): RelationalGroupedDataset = {
      df.groupBy(col.value, cols.map(_.value): _*)
    }

    /**
      * Create a multi-dimensional cube for the current Dataset using the
      * specified columns, so we can run aggregation on them.
      *
      * @group Group Dataframe operation
      * @see [[org.apache.spark.sql.Dataset.cube(cols:* org.apache.spark.sql.Dataset.cube]]
      */
    def cube(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkCube(df.toDF(), cols: _*).returnOrThrow("cube")
    }

    /**
      * Create a multi-dimensional cube for the current Dataset using the specified columns,
      * so we can run aggregation on them.
      *
      * This is a variant of cube that can only group by existing columns using column names
      * (i.e. cannot construct expressions).
      *
      * @example {{{
      *   // Compute the average for all numeric columns cubed by department and group.
      *   ds.cube("department".cname, "group".cname).avg()
      * }}}
      * @see [[doric.doc.DRelationalGroupedDataset]] for all the available aggregate functions.
      * @see [[org.apache.spark.sql.Dataset.cube(col1:* org.apache.spark.sql.Dataset.cube]]
      * @group Group Dataframe operation
      */
    @inline def cube(col: CName, cols: CName*): RelationalGroupedDataset = {
      df.cube(col.value, cols.map(_.value): _*)
    }

    /**
      * Create a multi-dimensional rollup for the current Dataset using the
      * specified columns, so we can run aggregation on them.
      *
      * @group Group Dataframe operation
      * @see [[org.apache.spark.sql.Dataset.rollup(cols:* org.apache.spark.sql.Dataset.rollup]]
      */
    def rollup(cols: DoricColumn[_]*): RelationalGroupedDataset = {
      sparkRollup(df.toDF(), cols: _*).returnOrThrow("rollup")
    }

    /**
      * Create a multi-dimensional rollup for the current Dataset using the specified columns,
      * so we can run aggregation on them.
      *
      * This is a variant of rollup that can only group by existing columns using column names
      * (i.e. cannot construct expressions).
      *
      * @example {{{
      *   // Compute the average for all numeric columns rolled up by department and group.
      *   ds.rollup("department".cname, "group".cname).avg()
      * }}}
      * @see [[doric.doc.DRelationalGroupedDataset]] for all the available aggregate functions.
      * @see [[org.apache.spark.sql.Dataset.rollup(col1:* org.apache.spark.sql.Dataset.rollup]]
      * @group Group Dataframe operation
      */
    @inline def rollup(col: CName, cols: CName*): RelationalGroupedDataset = {
      df.rollup(col.value, cols.map(_.value): _*)
    }
  }

  implicit class RelationalGroupedDatasetSem(rel: RelationalGroupedDataset) {

    /**
      * Compute aggregates by specifying a series of aggregate columns.
      *
      * @note this function by default retains the grouping columns in its output.
      *       To not retain grouping columns, set `spark.sql.retainGroupColumns` to false.
      * @group Group Dataframe operation
      * @todo scala doc
      * @see [[org.apache.spark.sql.Dataset.agg(expr:* org.apache.spark.sql.Dataset.agg]]
      */
    def agg(col: DoricColumn[_], cols: DoricColumn[_]*): DataFrame =
      sparkAgg(rel, col, cols: _*).returnOrThrow("agg")

    /**
      * Pivots a column of the current `DataFrame` and performs the specified
      * aggregation. There are two versions of pivot function:
      *   1. one that requires the caller to specify the list of distinct values
      *   to pivot on, and one that does not.
      *   1. The latter is more concise but less efficient, because Spark needs to
      *   first compute the list of distinct values internally.
      *
      * @group Group Dataframe operation
      * @param expr
      *   doric column to pivot
      * @param values
      *   the values of the column to extract
      * @tparam T
      *   The type of the column and parameters
      * @see [[org.apache.spark.sql.RelationalGroupedDataset.pivot(pivotColumn:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.RelationalGroupedDataset.pivot]]
      */
    def pivot[T](expr: DoricColumn[T])(
        values: Seq[T]
    ): RelationalGroupedDataset =
      sparkPivot(rel, expr, values).returnOrThrow("pivot")
  }

}
