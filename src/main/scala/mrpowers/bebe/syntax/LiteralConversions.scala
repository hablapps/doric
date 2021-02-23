package mrpowers.bebe.syntax

import mrpowers.bebe.{BooleanColumn, IntegerColumn, StringColumn}

trait LiteralConversions {

  implicit class Convinteger(int: Int) {
    def tc: IntegerColumn = IntegerColumn(int)
  }

  implicit class Convstring(str: String) {
    def tc: StringColumn = StringColumn(str)
  }

  implicit class Convboolean(bool: Boolean) {
    def tc: BooleanColumn = BooleanColumn(bool)
  }

}
