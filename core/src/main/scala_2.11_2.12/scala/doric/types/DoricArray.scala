package doric.types

import scala.collection.mutable

object DoricArray {
  type Collection[T] = mutable.WrappedArray[T]
}
