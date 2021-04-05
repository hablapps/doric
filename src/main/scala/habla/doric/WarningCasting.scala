package habla.doric

import cats.data.Kleisli

trait WarningCasting[From, To] {
  def cast(column: DoricColumn[From])(implicit constructor: FromDf[To]): DoricColumn[To]
}

object WarningCasting {
  @inline def apply[From, To](implicit imp: WarningCasting[From, To]): WarningCasting[From, To] =
    implicitly[WarningCasting[From, To]]
}
trait SparkWarningCasting[From, To] extends WarningCasting[From, To] {
  override def cast(column: DoricColumn[From])(implicit constructor: FromDf[To]): DoricColumn[To] =
    DoricColumn(column.toKleisli.map(_.cast(constructor.dataType)).run)
}
