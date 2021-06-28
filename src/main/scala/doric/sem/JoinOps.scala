package doric.sem

import cats.implicits._
import doric.{DoricColumn, DoricJoinColumn}

import org.apache.spark.sql.{DataFrame, Dataset}

trait JoinOps {
  implicit class DataframeJoinSyntax[A](df: Dataset[A]) {

    /**
      * Join with another `DataFrame`, using the given doric columns. The following performs
      * a full outer join between `df1` and `df2` by the key `dfKey` that must be string type.
      *
      * {{{
      *   df1.join(df2,"outer", colString("dfKey"))
      * }}}
      *
      * @param df2      Right side of the join.
      * @param joinType Type of join to perform. Default `inner`. Must be one of:
      *                 `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`,
      *                 `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`,
      *                 `semi`, `leftsemi`, `left_semi`, `anti`, `leftanti`, `left_anti`.
      * @param col      Doric column that must be in both dataframes.
      * @param cols     rest of doric columns that must be in both dataframes.
      */
    def join(
        df2: Dataset[_],
        joinType: String,
        col: DoricColumn[_],
        cols: DoricColumn[_]*
    ): DataFrame = {
      val elems = col +: cols.toList
      (
        elems
          .traverse(_.elem.run(df))
          .leftMap(_.map(JoinDoricSingleError(_, isLeft = true))),
        elems
          .traverse(_.elem.run(df2))
          .leftMap(_.map(JoinDoricSingleError(_, isLeft = false)))
      ).mapN((left, right) =>
        df.join(
          df2,
          left.zip(right).map(x => x._1 === x._2).reduce(_ && _),
          joinType
        )
      ).returnOrThrow("join")
    }

    /**
      * Join with another `DataFrame`, using the given doric columns. The following performs
      * a full outer join between `df1` with key `dfKey1` and `df2` with key `dfKey2` that must be string type.
      *
      * {{{
      *   val joinColumn = Left.colString("dfKey1") === Right.colString("dfKey2")
      *   df1.join(df2, joinColumn, "outer")
      * }}}
      *
      * @param df2      Right side of the join.
      * @param colum    Doric join column that must be in both dataframes.
      * @param joinType Type of join to perform. Default `inner`. Must be one of:
      *                 `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`,
      *                 `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`,
      *                 `semi`, `leftsemi`, `left_semi`, `anti`, `leftanti`, `left_anti`.
      */
    def join(
        df2: Dataset[_],
        colum: DoricJoinColumn,
        joinType: String
    ): DataFrame = {
      colum.elem
        .run((df, df2))
        .map(df.join(df2, _, joinType))
        .returnOrThrow("join")
    }

    /**
      * Join with another `DataFrame`, using the given doric columns. The following performs
      * a inner join between `df1` and `df2` by the key `dfKey` that must be string type.
      * It drops in the return dataframes the `dfKey` column of the right dataframe.
      *
      * {{{
      *   df1.innerJoinKeepLeftKeys(df2, colString("dfKey"))
      * }}}
      *
      * @param df2     Right side of the join.
      * @param column  Doric column that must be in both dataframes.
      * @param columns rest of doric columns that must be in both dataframes.
      */
    def innerJoinKeepLeftKeys(
        df2: Dataset[_],
        column: DoricColumn[_],
        columns: DoricColumn[_]*
    ): DataFrame = {
      val elems = column +: columns.toList
      (
        elems
          .traverse(_.elem.run(df.toDF()))
          .leftMap(_.map(JoinDoricSingleError(_, isLeft = true))),
        elems.traverse(
          _.elem
            .run(df2.toDF())
            .leftMap(_.map(JoinDoricSingleError(_, isLeft = false)))
        )
      ).mapN((left, right) => {
        val frameJoined = df.join(
          df2,
          left.zip(right).map(x => x._1 === x._2).reduce(_ && _),
          "inner"
        )
        right.foldLeft(frameJoined)(_.drop(_))
      }).returnOrThrow("innerJoinKeepLeftKeys")
    }
  }
}
