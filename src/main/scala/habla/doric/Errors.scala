package habla.doric

import cats.data.NonEmptyChain

case class DoricMultiError(errors: NonEmptyChain[Throwable]) extends Throwable(s"found ${errors.length} errors", errors.head.getCause) {
  override def getMessage: String = (s"found ${errors.length} errors" +: errors.map(_.getMessage)).iterator.mkString("\n")
}

case class DoricSingleError(message: String, cause: Throwable = null)(implicit val location: Location) extends Throwable(message, cause) {
  override def getMessage: String = message+s"\n\tlocated at . ${location.getLocation}"
}

object Location {
  implicit def location(implicit line: sourcecode.Line, file: sourcecode.FileName): Location =
    Location(file.value, line.value)
}

case class Location(fileName: sourcecode.FileName, lineNumber: sourcecode.Line) {
  def getLocation: String = s"(${fileName.value}:${lineNumber.value})"
}
