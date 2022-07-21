package doric
package types

import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag

trait SparkTypeLPI_II_Seq_Set_Specific {
  self: SparkTypeLPI_II =>

  implicit def fromSeq[A: ClassTag, C[X] <: Seq[X]](implicit
      st: SparkType[A],
      cbf: CanBuildFrom[_, A, C[A]]
  ): SparkType[C[A]] {
    type OriginalSparkType = DoricArray.Collection[st.OriginalSparkType]
  } = fromArray[A].customType(array => (cbf.apply ++= array).result)

  implicit def fromSet[A: ClassTag, C[X] <: Set[X]](implicit
      st: SparkType[A],
      cbf: CanBuildFrom[_, A, C[A]]
  ): SparkType[C[A]] {
    type OriginalSparkType = DoricArray.Collection[st.OriginalSparkType]
  } = fromArray[A].customType(array => (cbf.apply ++= array).result)

}
