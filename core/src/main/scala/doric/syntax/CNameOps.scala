package doric
package syntax

trait CNameOps {

  implicit class CNameOps(s: String) {
    @inline def cname: CName = CName(s)
  }

}
