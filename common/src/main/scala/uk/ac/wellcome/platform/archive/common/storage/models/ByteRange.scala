package uk.ac.wellcome.platform.archive.common.storage.models

import com.azure.storage.blob.models.BlobRange

sealed trait ByteRange

case class OpenByteRange(start: Long) extends ByteRange
case class ClosedByteRange(start: Long, count: Long) extends ByteRange

case object ByteRange {
  def apply(blobRange: BlobRange): ByteRange =
    if (blobRange.getCount == null) {
      OpenByteRange(blobRange.getOffset)
    } else {
      ClosedByteRange(blobRange.getOffset, blobRange.getCount)
    }
}
