package habla

import cats.data.{Kleisli, ValidatedNec}
import cats.implicits._
import habla.doric.syntax._
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}

package object doric extends AllSyntax {

  type DoricValidated[T] = ValidatedNec[DoricSingleError, T]
  type Doric[T]          = Kleisli[DoricValidated, Dataset[_], T]
  type DoricJoin[T]      = Kleisli[DoricValidated, (Dataset[_], Dataset[_]), T]

  // Basic types
  type BooleanColumn   = DoricColumn[Boolean]
  type StringColumn    = DoricColumn[String]
  type IntegerColumn   = DoricColumn[Int]
  type LongColumn      = DoricColumn[Long]
  type FloatColumn     = DoricColumn[Float]
  type DoubleColumn    = DoricColumn[Double]
  type DateColumn      = DoricColumn[Date]
  type LocalDateColumn = DoricColumn[LocalDate]
  type TimestampColumn = DoricColumn[Timestamp]
  type InstantColumn   = DoricColumn[Instant]
  type MapColumn[K, V] = DoricColumn[Map[K, V]]
  type ArrayColumn[A]  = DoricColumn[Array[A]]
  type DStructColumn   = DoricColumn[DStruct]

  implicit class DoricColumnops(elem: Doric[Column]) {
    def toDC[A]: DoricColumn[A] = DoricColumn(elem)
  }

  case class DoricJoinColumn(elem: DoricJoin[Column]) {
    def &&(other: DoricJoinColumn): DoricJoinColumn =
      (elem, other.elem).mapN(_ && _).toDJC
  }

  implicit class DoricJoinColumnOps(elem: DoricJoin[Column]) {
    def toDJC: DoricJoinColumn = DoricJoinColumn(elem)
  }

  implicit class DoricValidatedErrorHandler[T](dv: DoricValidated[T]) {
    def asLeftDfError: DoricValidated[T] =
      dv.leftMap(_.map(JoinDoricSingleError(_, isLeft = true)))
    def asRigthDfError: DoricValidated[T] =
      dv.leftMap(_.map(JoinDoricSingleError(_, isLeft = false)))
    def asSideDfError(isLeft: Boolean): DoricValidated[T] =
      dv.leftMap(_.map(JoinDoricSingleError(_, isLeft)))
  }

}
