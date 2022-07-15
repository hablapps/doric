package doric.types

import scala.collection.{BuildFrom, Factory}
import scala.reflect.ClassTag

trait SparkTypeLPI_II_Seq_Set_Specific { self: SparkTypeLPI_II =>

  implicit def fromSeq[A: ClassTag, O: ClassTag, C[X] <: Seq[X]](implicit
      st: SparkType[A] { type OriginalSparkType = O },
      fct: Factory[A, C[A]]
  ): SparkType[C[A]] {
    type OriginalSparkType = DoricArray.Collection[st.OriginalSparkType]
  } = fromArray[A, O].customType(array => fct.fromSpecific(array))

  implicit def fromSet[A: ClassTag, O: ClassTag, C[X] <: Set[X]](implicit
      st: SparkType[A] { type OriginalSparkType = O },
      fct: Factory[A, C[A]]
  ): SparkType[C[A]] {
    type OriginalSparkType = DoricArray.Collection[st.OriginalSparkType]
  } = fromArray[A, O].customType(array => fct.fromSpecific(array))
}
