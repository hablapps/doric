package habla.doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{DataFrame, Dataset}

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
        elems.traverse(_.elem.run(df.toDF())),
        elems.traverse(_.elem.run(df2.toDF()))
      )
        .mapN((left, right) =>
          df.join(
            df2,
            left.zip(right).map(x => x._1 === x._2).reduce(_ && _),
            joinType
          ).drop()
        )
        .returnOrThrow
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
  }

}
