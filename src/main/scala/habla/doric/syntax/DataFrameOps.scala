package habla.doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

trait DataFrameOps {

  implicit class DataframeSyntax[A](df: Dataset[A]) {

    private implicit class ErrorThrower[T](
        element: DoricValidated[Dataset[T]]
    ) {
      def returnOrThrow: Dataset[T] = if (element.isValid) {
        element.toEither.right.get
      } else {
        throw DoricMultiError(element.toEither.left.get)
      }
    }

    private implicit class JoinErrorThrower[T](
        element: DoricJoinValidated[Dataset[T]]
    ) {
      def returnOrThrow: Dataset[T] = if (element.isValid) {
        element.toEither.right.get
      } else {
        throw DoricJoinMultiError(element.toEither.left.get)
      }
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
        .returnOrThrow
    }

    def filter(condition: BooleanColumn): Dataset[A] = {
      condition.elem
        .run(df)
        .map(df.filter)
        .returnOrThrow
    }

    def where(condition: BooleanColumn): Dataset[A] = {
      condition.elem
        .run(df)
        .map(df.filter)
        .returnOrThrow
    }

    def select(col: DoricColumn[_]*): DataFrame = {
      col.toList
        .traverse(_.elem)
        .run(df.toDF())
        .map(df.select(_: _*))
        .returnOrThrow
    }

    def join(
        df2: Dataset[_],
        joinType: String,
        col: DoricColumn[_],
        cols: DoricColumn[_]*
    ): DataFrame = {
      val elems = col +: cols.toList
      (
        elems.traverse(_.elem.run(df.toDF())).leftMap(_.map(LeftDfError)),
        elems.traverse(_.elem.run(df2.toDF())).leftMap(_.map(RightDfError))
      ).mapN((left, right) =>
        df.join(
          df2,
          left.zip(right).map(x => x._1 === x._2).reduce(_ && _),
          joinType
        )
      ).returnOrThrow
    }

    def join(
        df2: Dataset[_],
        colum: DoricJoinColumn,
        joinType: String
    ): DataFrame = {
      colum.elem
        .run((df, df2))
        .map(df.join(df2, _, joinType))
        .returnOrThrow
    }

    def innerJoinKeepLeftKeys(
        df2: Dataset[_],
        column: DoricColumn[_],
        columns: DoricColumn[_]*
    ): DataFrame = {
      val elems = column +: columns.toList
      (
        elems.traverse(_.elem.run(df.toDF())).leftMap(_.map(LeftDfError)),
        elems.traverse(_.elem.run(df2.toDF()).leftMap(_.map(RightDfError)))
      ).mapN((left, right) => {
        val frameJoined = df.join(
          df2,
          left.zip(right).map(x => x._1 === x._2).reduce(_ && _),
          "inner"
        )
        right.foldLeft(frameJoined)(_.drop(_))
      }).returnOrThrow
    }

    def collectCols[T1: Encoder](col1: DoricColumn[T1]): Array[T1] = {
      df.select(col1).as[T1].collect()
    }

    def collectCols[T1: Encoder, T2: Encoder](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2]
    )(implicit fenc: Encoder[(T1, T2)]): Array[(T1, T2)] = {
      df.select(col1, col2).as[(T1, T2)].collect()
    }

    def collectCols[T1: Encoder, T2: Encoder, T3: Encoder](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3]
    )(implicit fenc: Encoder[(T1, T2, T3)]): Array[(T1, T2, T3)] = {
      df.select(col1, col2, col3).as[(T1, T2, T3)].collect()
    }

    def collectCols[T1: Encoder, T2: Encoder, T3: Encoder, T4: Encoder](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3],
        col4: DoricColumn[T4]
    )(implicit fenc: Encoder[(T1, T2, T3, T4)]): Array[(T1, T2, T3, T4)] = {
      df.select(col1, col2, col3, col4).as[(T1, T2, T3, T4)].collect()
    }
  }

}
