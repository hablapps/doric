package org.apache.spark.sql.doric

import cats.implicits.toTraverseOps
import doric.{DoricColumn, DoricValidated}

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.RelationalGroupedDataset.GroupType

/**
  * Interface to allow doric to access to some privet sql elements
  */
trait RelationalGroupedDatasetDoricInterface {

  private def aggregationType(
      groupType: GroupType,
      df: DataFrame,
      cols: DoricColumn[_]*
  ): DoricValidated[RelationalGroupedDataset] = {
    cols.toList
      .traverse(_.elem)
      .run(df)
      .map(x =>
        RelationalGroupedDataset(
          df.toDF(),
          x.map(_.expr),
          groupType
        )
      )
  }

  protected def sparkGroupBy(
      df: DataFrame,
      cols: DoricColumn[_]*
  ): DoricValidated[RelationalGroupedDataset] =
    aggregationType(RelationalGroupedDataset.GroupByType, df, cols: _*)

  protected def sparkRollup(
      df: DataFrame,
      cols: DoricColumn[_]*
  ): DoricValidated[RelationalGroupedDataset] =
    aggregationType(RelationalGroupedDataset.RollupType, df, cols: _*)

  protected def sparkCube(
      df: DataFrame,
      cols: DoricColumn[_]*
  ): DoricValidated[RelationalGroupedDataset] =
    aggregationType(RelationalGroupedDataset.CubeType, df, cols: _*)

  def sparkAgg(
      relationalGroupedDataset: RelationalGroupedDataset,
      expr: DoricColumn[_],
      exprs: DoricColumn[_]*
  ): DoricValidated[DataFrame] = {
    (expr +: exprs).toList
      .traverse(_.elem)
      .run(relationalGroupedDataset.df)
      .map(x => relationalGroupedDataset.agg(x.head, x.tail: _*))
  }

  def sparkPivot[T](
      relationalGroupedDataset: RelationalGroupedDataset,
      expr: DoricColumn[T],
      values: Seq[T]
  ): DoricValidated[RelationalGroupedDataset] = {
    expr.elem
      .run(relationalGroupedDataset.df)
      .map(x => relationalGroupedDataset.pivot(x, values))
  }

}
