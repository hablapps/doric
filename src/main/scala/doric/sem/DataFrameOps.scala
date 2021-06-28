package doric
package sem

import cats.implicits.{catsSyntaxTuple2Semigroupal, toTraverseOps}

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

trait DataFrameOps {

  implicit class DataframeSyntax[A](df: Dataset[A]) {

    private implicit class ErrorThrower[T](
        element: DoricValidated[Dataset[T]]
    ) {
      def returnOrThrow(functionType: String): Dataset[T] =
        element.fold(er => throw DoricMultiError(functionType, er), identity)
    }

    /**
      * Returns a new Dataset by adding a column or replacing the existing column that has
      * the same name.
      *
      * `column`'s expression must only refer to attributes supplied by this Dataset. It is an
      * error to add a column that refers to some other Dataset.
      *
      * @note this method introduces a projection internally. Therefore, calling it multiple times,
      *       for instance, via loops in order to add multiple columns can generate big plans which
      *       can cause performance issues and even `StackOverflowException`.
      */
    def withColumn[T](colName: String, col: DoricColumn[T]): DataFrame = {
      col.elem
        .run(df.toDF())
        .map(df.withColumn(colName, _))
        .returnOrThrow("withColumn")
    }

    /**
      * Filters rows using the given condition.
      * {{{
      *   // The following are equivalent:
      *   peopleDs.filter(colInt("age") > 15)
      *   peopleDs.where(colInt("age") > 15)
      * }}}
      * @param condition BooleanColumn that let pass elements that are true
      */
    def filter(condition: BooleanColumn): Dataset[A] = {
      condition.elem
        .run(df)
        .map(df.filter)
        .returnOrThrow("filter")
    }

    /**
      * Filters rows using the given condition.
      * {{{
      *   // The following are equivalent:
      *   peopleDs.filter(colInt("age") > 15)
      *   peopleDs.where(colInt("age") > 15)
      * }}}
      * @param condition BooleanColumn that let pass elements that are true
      */
    def where(condition: BooleanColumn): Dataset[A] = {
      condition.elem
        .run(df)
        .map(df.filter)
        .returnOrThrow("where")
    }

    /**
      * Selects a set of column based expressions.
      * {{{
      *   ds.select(colString("colA"), colInt("colB") + 1.lit)
      * }}}
      */
    def select(col: DoricColumn[_]*): DataFrame = {
      col.toList
        .traverse(_.elem)
        .run(df.toDF())
        .map(df.select(_: _*))
        .returnOrThrow("select")
    }

    /**
      * Join with another `DataFrame`, using the given doric columns. The following performs
      * a full outer join between `df1` and `df2` by the key `dfKey` that must be string type.
      *
      * {{{
      *   df1.join(df2,"outer", colString("dfKey"))
      * }}}
      *
      * @param df2 Right side of the join.
      * @param joinType Type of join to perform. Default `inner`. Must be one of:
      *                 `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`,
      *                 `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`,
      *                 `semi`, `leftsemi`, `left_semi`, `anti`, `leftanti`, `left_anti`.
      * @param col Doric column that must be in both dataframes.
      * @param cols rest of doric columns that must be in both dataframes.
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
      * @param df2 Right side of the join.
      * @param colum Doric join column that must be in both dataframes.
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
      * @param df2 Right side of the join.
      * @param column Doric column that must be in both dataframes.
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

    /**
      * Collects the provided columns of the dataframe
      * @param col1 the Doric column to collect from the dataframe
      * @tparam T1 the type of the column to collect, must have an Spark `Encoder`
      * @return The array of the selected column
      */
    def collectCols[T1: Encoder](col1: DoricColumn[T1]): Array[T1] = {
      df.select(col1).as[T1].collect()
    }

    /**
      * Collects the provided columns of the dataframe
      * @param col1 the Doric column to collect from the dataframe
      * @param col2 other Doric column to collect from the dataframe
      * @tparam T1 the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2 the type of the second column to collect, must have an Spark `Encoder`
      * @return The array of the selected columns
      */
    def collectCols[T1, T2](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2]
    )(implicit fenc: Encoder[(T1, T2)]): Array[(T1, T2)] = {
      df.select(col1, col2).as[(T1, T2)].collect()
    }

    /**
      * Collects the provided columns of the dataframe
      * @param col1 the Doric column to collect from the dataframe
      * @param col2 second Doric column to collect from the dataframe
      * @param col3 third Doric column to collect from the dataframe
      * @tparam T1 the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2 the type of the second column to collect, must have an Spark `Encoder`
      * @tparam T3 the type of the third column to collect, must have an Spark `Encoder`
      * @return The array of the selected columns
      */
    def collectCols[T1, T2, T3](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3]
    )(implicit fenc: Encoder[(T1, T2, T3)]): Array[(T1, T2, T3)] = {
      df.select(col1, col2, col3).as[(T1, T2, T3)].collect()
    }

    /**
      * Collects the provided columns of the dataframe
      * @param col1 the Doric column to collect from the dataframe
      * @param col2 second Doric column to collect from the dataframe
      * @param col3 third Doric column to collect from the dataframe
      * @param col4 forth Doric column to collect from the dataframe
      * @tparam T1 the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2 the type of the second column to collect, must have an Spark `Encoder`
      * @tparam T3 the type of the third column to collect, must have an Spark `Encoder`
      * @tparam T4 the type of the forth column to collect, must have an Spark `Encoder`
      * @return The array of the selected columns
      */
    def collectCols[T1, T2, T3, T4](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3],
        col4: DoricColumn[T4]
    )(implicit fenc: Encoder[(T1, T2, T3, T4)]): Array[(T1, T2, T3, T4)] = {
      df.select(col1, col2, col3, col4).as[(T1, T2, T3, T4)].collect()
    }
  }

}
