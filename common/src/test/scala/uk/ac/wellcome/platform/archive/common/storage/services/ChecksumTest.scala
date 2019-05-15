package uk.ac.wellcome.platform.archive.common.storage.services

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, SHA256}

import scala.util.Success

class ChecksumTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings {

  private def toInputStream(s: String) =
    IOUtils.toInputStream(s, "UTF-8");

  val algorithm = SHA256

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
