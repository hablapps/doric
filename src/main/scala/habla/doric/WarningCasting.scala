package habla.doric

trait WarningCasting[From, To] {
  def cast(column: From)(implicit constructor: FromDf[To], extractor: FromDf[From]): To
}

trait SparkWarningCasting[From, To] extends WarningCasting[From, To] {
  override def cast(column: From)(implicit constructor: FromDf[To], extractor: FromDf[From]): To =
    construct[To](column.sparkColumn.cast(constructor.dataType))
}
