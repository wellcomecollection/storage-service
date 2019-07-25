package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  SHA256
}
import uk.ac.wellcome.storage.streaming.Codec._

import scala.util.Success

class ChecksumTest
    extends FunSpec
    with Matchers
    with EitherValues
    with ScalaFutures
    with StorageRandomThings {

  private def toInputStream(s: String): InputStream =
    stringCodec.toStream(s).right.value

  val algorithm: SHA256.type = SHA256

  it("creates a useful string representation") {
    val checksum = Checksum(
      SHA256,
      ChecksumValue("1234567890")
    )

    checksum.toString shouldBe "sha256:1234567890"
  }

  it("calculates the checksum") {
    val content = "text"
    val inputStream = toInputStream(content)

    val expectedChecksum = Checksum(
      SHA256,
      ChecksumValue(
        "982d9e3eb996f559e633f4d194def3761d909f5a3b647d1a851fead67c32c9d1"
      )
    )

    val actualChecksumTry = Checksum.create(
      inputStream,
      algorithm
    )

    actualChecksumTry shouldBe a[Success[_]]
    actualChecksumTry.get shouldBe expectedChecksum
  }
}
