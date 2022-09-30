package doric
package syntax

import doric.sem.Location
import org.apache.spark.sql.{functions => f}

private[syntax] trait StringColumns31 {

  /**
    * Throws an exception with the provided error message.
    *
    * @throws java.lang.RuntimeException with the error message
    * @group String Type
    * @see [[org.apache.spark.sql.functions.raise_error]]
    */
  def raiseError(str: String)(implicit l: Location): NullColumn =
    str.lit.raiseError

  implicit class StringOperationsSyntax31(s: DoricColumn[String]) {

    /**
      * ********************************************************
      * MISC FUNCTIONS
      * ********************************************************
      */

    /**
      * Throws an exception with the provided error message.
      *
      * @throws java.lang.RuntimeException with the error message
      * @group String Type
      * @see [[org.apache.spark.sql.functions.raise_error]]
      */
    def raiseError(implicit l: Location): NullColumn =
      concat(
        s,
        "\n  at ".lit,
        l.fileName.value.lit,
        ":".lit,
        l.lineNumber.value.toString.lit
      ).elem.map(f.raise_error).toDC
  }
}
