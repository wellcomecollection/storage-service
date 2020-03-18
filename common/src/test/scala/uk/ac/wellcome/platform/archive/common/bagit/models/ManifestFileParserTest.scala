package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, VerifiableChecksum}
import uk.ac.wellcome.storage.streaming.Codec._

class ManifestFileParserTest extends FunSpec with Matchers with EitherValues with TryValues {
  it("parses a single SHA256 manifest") {
    val sha256 = asStream(
      """
        |abc123   file1.txt
        |def456   file2.txt
        |""".stripMargin)

    val files = ManifestFileParser.createFileLists(sha256 = sha256)
    
    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(sha256 = ChecksumValue("abc123")),
      BagPath("file2.txt") -> VerifiableChecksum(sha256 = ChecksumValue("def456")),
    )
  }

  private def asStream(s: String): InputStream =
    stringCodec.toStream(s).right.value
}
