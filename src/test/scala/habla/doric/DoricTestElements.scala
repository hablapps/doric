package habla.doric

import org.scalatest.funspec.AnyFunSpecLike

private[doric] trait DoricTestElements
    extends AnyFunSpecLike
    with SparkSessionTestWrapper
    with TypedColumnTest {}
