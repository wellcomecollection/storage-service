package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.Readable

trait SizeFinder[Ident] extends Readable[Ident, Long] {
  def getSize(ident: Ident): Either[ReadError, Long] =
    get(ident) match {
      case Right(Identified(_, size)) => Right(size)
      case Left(err)                  => Left(err)
    }
}
