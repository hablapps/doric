package doric
package sem

import doric.types.SparkType

import org.apache.spark.sql.Dataset

private[sem] trait CollectOps {

  implicit class CollectSyntax[A](df: Dataset[A]) {

    /**
      * Collects the provided columns of the dataframe
      *
      * @group Action operation
      * @param col1
      * the Doric column to collect from the dataframe
      * @tparam T1
      * the type of the column to collect, must have an Spark `Encoder`
      * @return
      * The array of the selected column
      */
    def collectCols[T1](
        col1: DoricColumn[T1]
    )(implicit st1: SparkType[T1]): List[T1] = {
      df.select(col1)
        .collect()
        .iterator
        .map(row =>
          st1
            .rowTransformT(row.get(0))
        )
        .toList
    }

    /**
      * Collects the provided columns of the dataframe
      *
      * @group Action operation
      * @param col1
      * the Doric column to collect from the dataframe
      * @param col2
      * other Doric column to collect from the dataframe
      * @return
      * The array of the selected columns
      */
    def collectCols[T1, T2](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2]
    )(implicit st1: SparkType[T1], st2: SparkType[T2]): List[(T1, T2)] = {
      df.select(col1, col2)
        .collect()
        .iterator
        .map(row =>
          (
            st1.rowTransformT(row.get(0)),
            st2.rowTransformT(row.get(1))
          )
        )
        .toList
    }

    /**
      * Collects the provided columns of the dataframe
      *
      * @group Action operation
      * @param col1
      * the Doric column to collect from the dataframe
      * @param col2
      * second Doric column to collect from the dataframe
      * @param col3
      * third Doric column to collect from the dataframe
      * @return
      * The array of the selected columns
      */
    def collectCols[T1, T2, T3](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3]
    )(implicit
        st1: SparkType[T1],
        st2: SparkType[T2],
        st3: SparkType[T3]
    ): List[(T1, T2, T3)] = {
      df.select(col1, col2, col3)
        .collect()
        .map(row =>
          (
            st1.rowTransformT(row.get(0)),
            st2.rowTransformT(row.get(1)),
            st3.rowTransformT(row.get(2))
          )
        )
        .toList
    }

    /**
      * Collects the provided columns of the dataframe
      *
      * @group Action operation
      * @param col1
      * the Doric column to collect from the dataframe
      * @param col2
      * second Doric column to collect from the dataframe
      * @param col3
      * third Doric column to collect from the dataframe
      * @param col4
      * forth Doric column to collect from the dataframe
      * @return
      * The array of the selected columns
      */
    def collectCols[T1, T2, T3, T4](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2],
        col3: DoricColumn[T3],
        col4: DoricColumn[T4]
    )(implicit
        st1: SparkType[T1],
        st2: SparkType[T2],
        st3: SparkType[T3],
        st4: SparkType[T4]
    ): List[(T1, T2, T3, T4)] = {
      df.select(col1, col2, col3, col4)
        .collect()
        .map(row =>
          (
            st1.rowTransformT(row.get(0)),
            st2.rowTransformT(row.get(1)),
            st3.rowTransformT(row.get(2)),
            st4.rowTransformT(row.get(3))
          )
        )
        .toList
    }
  }

}
