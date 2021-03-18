package habla.doric

trait Casting[From, To] {
  def cast(column: From)(implicit constructor: FromDf[To], extractor: FromDf[From]): To
}

trait SparkCasting[From, To] extends Casting[From, To] {
  override def cast(column: From)(implicit constructor: FromDf[To], extractor: FromDf[From]): To =
    construct[To](column.sparkColumn.cast(constructor.dataType))
}
