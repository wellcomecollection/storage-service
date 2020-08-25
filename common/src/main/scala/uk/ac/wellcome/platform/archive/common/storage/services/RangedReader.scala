package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.storage.models.ByteRange
import uk.ac.wellcome.storage.ReadError

trait RangedReader[Ident] {
  def getBytes(id: Ident, range: ByteRange): Either[ReadError, Array[Byte]]
}
