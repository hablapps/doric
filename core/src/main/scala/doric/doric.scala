import cats.data.{EitherNec, Kleisli, ValidatedNec}
import cats.implicits._
import cats.Parallel
import cats.arrow.FunctionK
import doric.sem.DoricSingleError
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset, Row}

package object doric extends syntax.All with sem.All {

  lazy val minorScalaVersion: Int = {
    val minorScalaVersionRegexp = """[^.]*\.([^.]*)\..*""".r
    val minorScalaVersionRegexp(minorScalaVersionStr) =
      util.Properties.versionNumberString
    minorScalaVersionStr.toInt
  }

  type DoricValidated[T] = ValidatedNec[DoricSingleError, T]
  type Doric[T]          = Kleisli[DoricValidated, Dataset[_], T]
  type DoricJoin[T]      = Kleisli[DoricValidated, (Dataset[_], Dataset[_]), T]

  object Doric {

    def apply[T](a: T): Doric[T] =
      Kleisli[DoricValidated, Dataset[_], T] { _ =>
        a.valid
      }

    def apply[T](f: Dataset[_] => T): Doric[T] =
      Kleisli[DoricValidated, Dataset[_], T] { df =>
        f(df).valid
      }
  }

  private[doric] type DoricEither[A] = EitherNec[DoricSingleError, A]

  private[doric] val toValidated = new FunctionK[DoricEither, DoricValidated] {
    override def apply[A](fa: DoricEither[A]): DoricValidated[A] =
      fa.toValidated
  }
  private[doric] val toEither = new FunctionK[DoricValidated, DoricEither] {
    override def apply[A](fa: DoricValidated[A]): DoricEither[A] = fa.toEither
  }

  private type SequenceDoric[F] = Kleisli[DoricEither, Dataset[_], F]

  implicit private[doric] class SeqPar[A](a: Doric[A])(implicit
      P: Parallel.Aux[SequenceDoric, Doric]
  ) {

    def seqFlatMap[B](f: A => Doric[B]): Doric[B] = {
      P.parallel(P.flatMap.flatMap(P.sequential(a))(x => P.sequential(f(x))))
    }
  }

  // Basic types
  type NullColumn      = DoricColumn[Null]
  type BooleanColumn   = DoricColumn[Boolean]
  type StringColumn    = DoricColumn[String]
  type ByteColumn      = DoricColumn[Byte]
  type IntegerColumn   = DoricColumn[Int]
  type LongColumn      = DoricColumn[Long]
  type FloatColumn     = DoricColumn[Float]
  type DoubleColumn    = DoricColumn[Double]
  type BinaryColumn    = DoricColumn[Array[Byte]]
  type DateColumn      = DoricColumn[Date]
  type LocalDateColumn = DoricColumn[LocalDate]
  type TimestampColumn = DoricColumn[Timestamp]
  type InstantColumn   = DoricColumn[Instant]
  type MapColumn[K, V] = DoricColumn[Map[K, V]]
  type ArrayColumn[A]  = DoricColumn[Array[A]]
  type RowColumn       = DoricColumn[Row]

  private[doric] implicit class DoricColumnops(elem: Doric[Column]) {
    @inline def toDC[A]: DoricColumn[A] = DoricColumn(elem)
  }

  private[doric] implicit class DoricJoinColumnOps(elem: DoricJoin[Column]) {
    @inline def toDJC: DoricJoinColumn = DoricJoinColumn(elem)
  }

  private[doric] implicit class DoricValidatedErrorHandler[T](
      val dv: DoricValidated[T]
  ) extends AnyVal {
    final def asSideDfError(isLeft: Boolean): DoricValidated[T] =
      dv.leftMap(_.map(sem.JoinDoricSingleError(_, isLeft)))
  }

  implicit class StringIntCNameOps(val sc: StringContext) extends AnyVal {
    final def c(args: Any*): CName =
      CName(
        sc.parts.iterator
          .zipAll(args.iterator.map(_.toString), "", "")
          .map { case (a, b) => a + b }
          .mkString
      )
  }

}
