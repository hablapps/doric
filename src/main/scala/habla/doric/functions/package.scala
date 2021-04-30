package habla.doric

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}

package object functions {
  def mapFromArrays[K, V](
      keys: DoricColumn[Array[K]],
      values: DoricColumn[Array[V]]
  ): DoricColumn[Map[K, V]] = {
    (keys.elem, values.elem).mapN((k, v) => f.map_from_arrays(k, v)).toDC
  }

  def map[K, V](
      first: (DoricColumn[K], DoricColumn[V]),
      rest: (DoricColumn[K], DoricColumn[V])*
  ): DoricColumn[Map[K, V]] = {
    val list: List[Doric[Column]] =
      List(first._1.elem, first._2.elem) ++ rest.flatMap(x => List(x._1.elem, x._2.elem))
    list.sequence.map(x => f.map(x: _*)).toDC
  }

  def concat(cols: DoricColumn[String]*): DoricColumn[String] =
    cols.map(_.elem).toList.sequence.map(f.concat(_: _*)).toDC

  def concatArrays[T](cols: DoricColumn[Array[T]]*): DoricColumn[Array[T]] =
    cols.map(_.elem).toList.sequence.map(f.concat(_: _*)).toDC
}
