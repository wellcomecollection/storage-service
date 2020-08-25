package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.storage.models.{ByteRange, ClosedByteRange, OpenByteRange}

object ByteRangeUtil {
  // Partitions an object of size 1..length into ranges of size at most `bufferSize`
  def partition(length: Long, bufferSize: Long): Seq[ByteRange] =
    ((0L to length - 1) by bufferSize)
      .map { offset: Long =>
        if (offset + bufferSize > length)
          OpenByteRange(offset)
        else
          ClosedByteRange(offset, bufferSize)
      }
}
