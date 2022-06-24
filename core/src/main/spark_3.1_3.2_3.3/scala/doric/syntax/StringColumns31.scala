package doric
package syntax

import org.apache.spark.sql.{functions => f}

private[syntax] trait StringColumns31 {

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
    def raiseError: NullColumn = s.elem.map(f.raise_error).toDC
  }
}
