package uk.ac.wellcome.platform.archive.common.verify

import java.io.InputStream
import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex

import scala.util.Try

case class HashingResult(
  md5: ChecksumValue,
  sha1: ChecksumValue,
  sha256: ChecksumValue,
  sha512: ChecksumValue
)

object Hasher {

  /** Given an InputStream, read the complete contents and hash it using the four
    * algorithms suggested by the BagIt spec.
    *
    */
  def hash(inputStream: InputStream): Try[HashingResult] = Try {
    val digest_MD5: MessageDigest = MessageDigest.getInstance(MD5.value)
    val digest_SHA1 = MessageDigest.getInstance(SHA1.value)
    val digest_SHA256 = MessageDigest.getInstance(SHA256.value)
    val digest_SHA512 = MessageDigest.getInstance(SHA512.value)

    // This implementation is based on MessageDigest.updateDigest(), but rather than
    // updating a single digest, we update all four at once.
    val STREAM_BUFFER_LENGTH = 1024

    val buffer = new Array[Byte](STREAM_BUFFER_LENGTH)
    var read = inputStream.read(buffer, 0, STREAM_BUFFER_LENGTH)

    while (read > -1) {
      digest_MD5.update(buffer, 0, read)
      digest_SHA1.update(buffer, 0, read)
      digest_SHA256.update(buffer, 0, read)
      digest_SHA512.update(buffer, 0, read)

      read = inputStream.read(buffer, 0, STREAM_BUFFER_LENGTH)
    }
    // == MessageDigest.updateDigest() ends ==

    HashingResult(
      md5 = asChecksumValue(digest_MD5),
      sha1 = asChecksumValue(digest_SHA1),
      sha256 = asChecksumValue(digest_SHA256),
      sha512 = asChecksumValue(digest_SHA512),
    )
  }

  private def asChecksumValue(digest: MessageDigest): ChecksumValue =
    ChecksumValue(Hex.encodeHexString(digest.digest))
}
