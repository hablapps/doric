package doric.syntax

import doric.sem.SparkErrorWrapper

object ChildColumnNotFound {
  def apply(expectedCol: String, foundCols: List[String]): SparkErrorWrapper = {
    SparkErrorWrapper(
      new Throwable(
        s"No such struct field $expectedCol in ${foundCols.mkString(", ")}"
      )
    )
  }
}
