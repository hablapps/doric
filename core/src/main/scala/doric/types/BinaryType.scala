package doric.types

trait BinaryType[T] {}

object BinaryType {
  def apply[T]: BinaryType[T] = new BinaryType[T] {}

  implicit val stringBinary: BinaryType[String] = BinaryType[String]

  implicit val arrayByteBinary: BinaryType[Array[Byte]] =
    BinaryType[Array[Byte]]

  implicit val arrayCharBinary: BinaryType[Array[Char]] =
    BinaryType[Array[Char]]

}
