package weco.storage_service.storage.services

import java.io.InputStream

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.checksum.{Checksum, ChecksumValue, SHA256}
import weco.storage.streaming.Codec._

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
}
