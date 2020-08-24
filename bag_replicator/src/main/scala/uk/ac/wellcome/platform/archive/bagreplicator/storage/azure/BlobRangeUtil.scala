package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import java.util.Base64

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

  // Create the block IDs required by the Azure Put Block API.
  //
  // See https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
  def getBlockIdentifiers(count: Int): Seq[String] = {

    // How many digits are there in the string representation of `count`?
    val identifierLength = count.toString.length

    // There doesn't seem to be a firm rule about what the identifiers are, as
    // long as they're all distinct and the same length.
    //
    // For simplicity, use 1 2 3 ... N
    //
    // These identifiers then get base64 encoded.
    (1 to count)
      .map { zeroPad(_, length = identifierLength) }
      .map { _.getBytes }
      .map { Base64.getEncoder.encodeToString }
  }

  private def zeroPad(number: Int, length: Int): String =
    ("0" * length + number.toString).takeRight(length)
}
