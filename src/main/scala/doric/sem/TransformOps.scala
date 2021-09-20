package doric
package sem

import cats.implicits.toTraverseOps

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.doric.DataFrameExtras

trait TransformOps {

  implicit class DataframeTransformationSyntax[A](df: Dataset[A]) {

    /**
      * Returns a new Dataset by adding a column or replacing the existing
      * column that has the same name.
      *
      * `column`'s expression must only refer to attributes supplied by this
      * Dataset. It is an error to add a column that refers to some other
      * Dataset.
      *
      * @note
      *   this method introduces a projection internally. Therefore, calling it
      *   multiple times, for instance, via loops in order to add multiple
      *   columns can generate big plans which can cause performance issues and
      *   even `StackOverflowException`.
      */
    def withColumn(colName: String, col: DoricColumn[_]): DataFrame = {
      col.elem
        .run(df.toDF())
        .map(df.withColumn(colName, _))
        .returnOrThrow("withColumn")
    }

    /**
      * Returns a new dataset by adding all columns, or replacing the existing
      * columns that has the same name. If a column name is twice in the same
      * 'withColumns' this method will throw an exception.
      *
      * @param namesAndCols
      *   tuples of name and column expression
      */
    def withColumns(
        namesAndCols: (String, DoricColumn[_])*
    ): DataFrame = {
      if (namesAndCols.isEmpty) df.toDF
      else
        namesAndCols.toList
          .traverse(_._2.elem)
          .run(df)
          .map(DataFrameExtras.withColumnsE(df, namesAndCols.map(_._1), _))
          .returnOrThrow("withColumns")
    }

    /**
      * Returns a new dataset by adding all columns, or replacing the existing
      * columns that has the same name. If a column name is twice in the same
      * 'withColumns' this method will throw an exception.
      *
      * @param namesAndCols
      *   tuples of name and column expression
      */
    def withColumns(
        namesAndCols: Map[String, DoricColumn[_]]
    ): DataFrame = {
      if (namesAndCols.isEmpty) df.toDF
      else
        withColumns(namesAndCols.toList: _*)
    }

    /**
      * Filters rows using the given condition.
      * {{{
      *   // The following are equivalent:
      *   peopleDs.filter(colInt("age") > 15)
      *   peopleDs.where(colInt("age") > 15)
      * }}}
      *
      * @param condition
      *   BooleanColumn that let pass elements that are true
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
      *
      * @param condition
      *   BooleanColumn that let pass elements that are true
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
  }
}
