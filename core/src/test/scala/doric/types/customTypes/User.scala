package doric
package types
package customTypes

import scala.util.Try

case class User(name: String, age: Int)

object User {

  object ToInt {
    def unapply(s: String): Option[Int] =
      Try(s.toInt).toOption
  }
  implicit val userst: SparkType[User] {
    type OriginalSparkType = String
  } =
    SparkType[String].customType[User](x => {
      val name :: ToInt(age) :: Nil = x.split("#").toList
      User(name, age)
    })

  implicit val userlst: LiteralSparkType[User] {
    type OriginalSparkType = String
  } =
    LiteralSparkType[String].customType[User](x =>
      s"${x.name}#${x.age.toString}"
    )
}
