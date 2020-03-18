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

  it("parses a SHA256 and an MD5 manifest") {
    val md5 = asStream(
      """
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream(
      """
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files = ManifestFileParser.createFileLists(md5 = Some(md5), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      ),
    )
  }

  it("parses a SHA256 and an SHA1 manifest") {
    val sha1 = asStream(
      """
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream(
      """
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files = ManifestFileParser.createFileLists(sha1 = Some(sha1), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        sha1 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        sha1 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      ),
    )
  }

  it("parses a SHA256 and an SHA512 manifest") {
    val sha512 = asStream(
      """
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream(
      """
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files = ManifestFileParser.createFileLists(sha512 = Some(sha512), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        sha512 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        sha512 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      ),
    )
  }

  private def asStream(s: String): InputStream =
    stringCodec.toStream(s).right.value
}
