package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import java.util.Base64

import com.azure.storage.blob.models.BlobRange
import uk.ac.wellcome.platform.archive.common.storage.models.{ClosedByteRange, OpenByteRange}
import uk.ac.wellcome.platform.archive.common.storage.services.ByteRangeUtil

object BlobRangeUtil {
  def getRanges(length: Long, blockSize: Long): Seq[BlobRange] =
    ByteRangeUtil.partition(length, blockSize).map {
      case OpenByteRange(start)          => new BlobRange(start)
      case ClosedByteRange(start, count) => new BlobRange(start, count)
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
