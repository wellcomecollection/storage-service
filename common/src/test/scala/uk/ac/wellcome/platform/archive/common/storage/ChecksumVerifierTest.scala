package uk.ac.wellcome.platform.archive.common.storage

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.util.{Failure, Success}

class ChecksumVerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings {

  private def toInputStream(s: String) =
    IOUtils.toInputStream(s, "UTF-8");

  val algorithm = MessageDigestAlgorithms.SHA_256

  it("calculates the checksum") {
    val content = "text"
    val inputStream = toInputStream(content)

    val expectedChecksum =
      "982d9e3eb996f559e633f4d194def3761d909f5a3b647d1a851fead67c32c9d1"

    val actualChecksumTry = ChecksumVerifier.checksum(
      inputStream,
      algorithm
    )

    actualChecksumTry shouldBe a[Success[_]]
    actualChecksumTry.get shouldBe expectedChecksum
  }

  it("fails for an unknown algorithm") {
    val actualChecksumTry = ChecksumVerifier.checksum(
      toInputStream(randomAlphanumeric()),
      algorithm = "unknown"
    )

    actualChecksumTry shouldBe a[Failure[_]]

    actualChecksumTry.failed.get shouldBe a[RuntimeException]
  }
}
