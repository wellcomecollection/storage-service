package weco.storage.services

import weco.storage._
import weco.storage.store.Readable

trait SizeFinder[Ident] extends Readable[Ident, Long] {
  def getSize(ident: Ident): Either[ReadError, Long] =
    get(ident) match {
      case Right(Identified(_, size)) => Right(size)
      case Left(err)                  => Left(err)
    }
}
