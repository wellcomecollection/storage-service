package weco.storage.services

import weco.storage.ReadError
import weco.storage.models.ByteRange
import weco.storage.models.ByteRange

trait RangedReader[Ident] {
  def getBytes(id: Ident, range: ByteRange): Either[ReadError, Array[Byte]]
}
