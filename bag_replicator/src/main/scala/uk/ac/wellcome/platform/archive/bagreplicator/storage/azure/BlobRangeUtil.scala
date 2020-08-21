package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import com.azure.storage.blob.models.BlobRange

object BlobRangeUtil {
  def getRanges(length: Long, blockSize: Long): Seq[BlobRange] =
    ((0L to length - 1) by blockSize)
      .map { offset: Long =>
        if (offset + blockSize > length)
          new BlobRange(offset)
        else
          new BlobRange(offset, blockSize)
      }
}
