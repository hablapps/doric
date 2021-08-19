package doric
package sem

import org.apache.spark.sql.{Dataset, Encoder}

trait CollectOps {

  implicit class CollectSyntax[A](df: Dataset[A]) {

    /**
      * Collects the provided columns of the dataframe
      * @param col1
      *   the Doric column to collect from the dataframe
      * @tparam T1
      *   the type of the column to collect, must have an Spark `Encoder`
      * @return
      *   The array of the selected column
      */
    def collectCols[T1: Encoder](col1: DoricColumn[T1]): Array[T1] = {
      df.select(col1).as[T1].collect()
    }

    /**
      * Collects the provided columns of the dataframe
      * @param col1
      *   the Doric column to collect from the dataframe
      * @param col2
      *   other Doric column to collect from the dataframe
      * @tparam T1
      *   the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2
      *   the type of the second column to collect, must have an Spark `Encoder`
      * @return
      *   The array of the selected columns
      */
    def collectCols[T1, T2](
        col1: DoricColumn[T1],
        col2: DoricColumn[T2]
    )(implicit fenc: Encoder[(T1, T2)]): Array[(T1, T2)] = {
      df.select(col1, col2).as[(T1, T2)].collect()
    }

    /**
      * Collects the provided columns of the dataframe
      * @param col1
      *   the Doric column to collect from the dataframe
      * @param col2
      *   second Doric column to collect from the dataframe
      * @param col3
      *   third Doric column to collect from the dataframe
      * @tparam T1
      *   the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2
      *   the type of the second column to collect, must have an Spark `Encoder`
      * @tparam T3
      *   the type of the third column to collect, must have an Spark `Encoder`
      * @return
      *   The array of the selected columns
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
      * @param col1
      *   the Doric column to collect from the dataframe
      * @param col2
      *   second Doric column to collect from the dataframe
      * @param col3
      *   third Doric column to collect from the dataframe
      * @param col4
      *   forth Doric column to collect from the dataframe
      * @tparam T1
      *   the type of the column to collect, must have an Spark `Encoder`
      * @tparam T2
      *   the type of the second column to collect, must have an Spark `Encoder`
      * @tparam T3
      *   the type of the third column to collect, must have an Spark `Encoder`
      * @tparam T4
      *   the type of the forth column to collect, must have an Spark `Encoder`
      * @return
      *   The array of the selected columns
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
