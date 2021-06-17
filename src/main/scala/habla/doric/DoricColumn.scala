package habla.doric

import cats.data.{Kleisli, Validated}
import cats.Applicative
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxTuple2Semigroupal, catsSyntaxValidatedId, catsSyntaxValidatedIdBinCompat0}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.{Column, Dataset}

case class DoricColumn[T](elem: Doric[Column])

object DoricColumn {
  def apply[T](f: Dataset[_] => DoricValidated[Column]): DoricColumn[T] =
    DoricColumn(Kleisli(f))

  def apply[T](col: Column): DoricColumn[T] = {
    Kleisli[DoricValidated, Dataset[_], Column]((_: Dataset[_]) => col.valid)
  }.toDC
}

object DoricColumnExtr {
  def unapply[A: SparkType](
      column: Column
  )(implicit ap: Applicative[Doric]): Option[DoricColumn[A]] = {
    if (SparkType[A].isValid(column.expr.dataType))
      Some(column.pure[Doric].toDC)
    else
      None
  }
}

sealed abstract class SideDoricColumn[T] protected (val elem: Doric[Column]) {
  type OtherSide <: SideDoricColumn[T]
  def ===(otherElem: OtherSide): DoricJoinColumn = {
    val isLeft = this.isInstanceOf[LeftDoricColumn[T]]

    Kleisli[DoricValidated, (Dataset[_], Dataset[_]), Column](dfs => {
      (
        elem.run(dfs._1).asSideDfError(isLeft),
        otherElem.elem.run(dfs._2).asSideDfError(!isLeft)
      ).mapN(_ === _)
    }).toDJC
  }
}

case class LeftDoricColumn[T](override val elem: Doric[Column])
    extends SideDoricColumn[T](elem) {
  override type OtherSide = RightDoricColumn[T]
}

case class RightDoricColumn[T](override val elem: Doric[Column])
    extends SideDoricColumn[T](elem) {
  override type OtherSide = LeftDoricColumn[T]
}

object LeftDF extends ColGetters[LeftDoricColumn] {
  @inline override protected def constructSide[T](
      column: Doric[Column]
  ): LeftDoricColumn[T] =
    LeftDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): LeftDoricColumn[T] =
    constructSide(doricColumn.elem)
}

object RightDF extends ColGetters[RightDoricColumn] {
  @inline override protected def constructSide[T](
      column: Doric[Column]
  ): RightDoricColumn[T] =
    RightDoricColumn(column)

  def apply[T](doricColumn: DoricColumn[T]): RightDoricColumn[T] =
    constructSide(doricColumn.elem)
}

private[doric] trait ColGetters[F[_]] {
  @inline protected def constructSide[T](column: Doric[Column]): F[T]

  def col[T: SparkType](colName: String)(implicit
      location: Location
  ): F[T] =
    constructSide(SparkType[T].validate(colName))

  def colString(colName: String)(implicit
      location: Location
  ): F[String] =
    col[String](colName)

  def colInt(colName: String)(implicit
      location: Location
  ): F[Int] =
    col[Int](colName)

  def colLong(colName: String)(implicit
      location: Location
  ): F[Long] =
    col[Long](colName)

  def colInstant(colName: String)(implicit
      location: Location
  ): F[Instant] =
    col[Instant](colName)

  def colLocalDate(colName: String)(implicit
      location: Location
  ): F[LocalDate] =
    col[LocalDate](colName)

  def colTimestamp(colName: String)(implicit
      location: Location
  ): F[Timestamp] =
    col[Timestamp](colName)

  def colDate(colName: String)(implicit location: Location): F[Date] =
    col[Date](colName)

  def colArray[T: SparkType](colName: String)(implicit
      location: Location
  ): F[Array[T]] =
    col[Array[T]](colName)

  def colArrayInt(colName: String)(implicit
      location: Location
  ): F[Array[Int]] =
    col[Array[Int]](colName)

  def colArrayString(colName: String)(implicit
      location: Location
  ): F[Array[String]] =
    col[Array[String]](colName)

  def colStruct(colName: String)(implicit location: Location): F[DStruct] =
    col[DStruct](colName)

  def colFromDF[T: SparkType](colName: String, originDF: Dataset[_])(implicit
      location: Location
  ): F[T] = {
    val doricColumn = Kleisli[DoricValidated, Dataset[_], Column](df => {
      val result = SparkType[T].validate(colName).run(originDF)
      if (result.isValid) {
        try {
          val column: Column = result.toEither.right.get
          val head           = df.select(column).schema.head
          if (SparkType[T].isValid(head.dataType))
            Validated.valid(column)
          else
            ColumnTypeError(
              head.name,
              SparkType[T].dataType,
              head.dataType
            ).invalidNec
        } catch {
          case e: Throwable => SparkErrorWrapper(e).invalidNec
        }
      } else {
        result
      }
    })
    constructSide[T](doricColumn)
  }
}
