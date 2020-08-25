package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.storage.models.ByteRange

trait RangedReader[Ident] {
  def getBytes(id: Ident, range: ByteRange): Array[Byte]
}
