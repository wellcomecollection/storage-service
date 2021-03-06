package weco.storage_service.storage.services

import java.io.InputStream

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.verify.{Checksum, ChecksumValue, SHA256}
import weco.storage.streaming.Codec._

import scala.util.Success

class ChecksumTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with ScalaFutures
    with StorageRandomGenerators {

  private def toInputStream(s: String): InputStream =
    stringCodec.toStream(s).value

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
