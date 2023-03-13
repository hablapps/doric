package doric

sealed trait Order
object Asc  extends Order
object Desc extends Order

case class CNameOrd(name: CName, order: Order)

object CNameOrd {
  private final lazy val defaultOrder = Asc

  def apply(name: String, order: Order): CNameOrd = this(CName(name), order)

  def apply(name: String): CNameOrd = this(CName(name), defaultOrder)

  implicit def colName2OrderedCol(colName: CName): CNameOrd =
    CNameOrd(colName, defaultOrder)
}
