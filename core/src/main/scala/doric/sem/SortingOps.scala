package doric.sem

import cats.syntax.traverse._
import doric.DoricColumn
import org.apache.spark.sql.DataFrame

private[sem] trait SortingOps {

  implicit class DataframeSortSyntax(df: DataFrame) {

    def sort(col: DoricColumn[_]*): DataFrame =
      col.toList
        .traverse(_.elem)
        .run(df)
        .map(col => df.orderBy(col: _*))
        .returnOrThrow("sort")

    def orderBy(col: DoricColumn[_]*): DataFrame = sort(col: _*)
  }
}
