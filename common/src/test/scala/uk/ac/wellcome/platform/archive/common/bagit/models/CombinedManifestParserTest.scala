package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.verify.{
  ChecksumValue,
  VerifiableChecksum
}
import uk.ac.wellcome.storage.streaming.Codec._

class CombinedManifestParserTest
    extends FunSpec
    with Matchers
    with EitherValues
    with TryValues {
  it("parses a single SHA256 manifest") {
    val sha256 = asStream("""
        |abc123   file1.txt
        |def456   file2.txt
        |""".stripMargin)

    val files = CombinedManifestParser.createFileLists(sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        sha256 = ChecksumValue("abc123")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        sha256 = ChecksumValue("def456")
      )
    )
  }

  it("parses a SHA256 and an MD5 manifest") {
    val md5 = asStream("""
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream("""
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files =
      CombinedManifestParser.createFileLists(md5 = Some(md5), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      )
    )
  }

  it("parses a SHA256 and an SHA1 manifest") {
    val sha1 = asStream("""
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream("""
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files =
      CombinedManifestParser.createFileLists(sha1 = Some(sha1), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        sha1 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        sha1 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      )
    )
  }

  it("parses a SHA256 and an SHA512 manifest") {
    val sha512 = asStream("""
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream("""
        |bbb111   file1.txt
        |bbb222   file2.txt
        |""".stripMargin)

    val files = CombinedManifestParser.createFileLists(
      sha512 = Some(sha512),
      sha256 = sha256
    )

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        sha512 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        sha512 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      )
    )
  }

  it("parses multiple manifests, even if the files are in different orders") {
    val md5 = asStream("""
        |aaa333   file3.txt
        |aaa111   file1.txt
        |aaa222   file2.txt
        |""".stripMargin)

    val sha256 = asStream("""
        |bbb222   file2.txt
        |bbb333   file3.txt
        |bbb111   file1.txt
        |""".stripMargin)

    val files =
      CombinedManifestParser.createFileLists(md5 = Some(md5), sha256 = sha256)

    files.success.value shouldBe Map(
      BagPath("file1.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa111")),
        sha256 = ChecksumValue("bbb111")
      ),
      BagPath("file2.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa222")),
        sha256 = ChecksumValue("bbb222")
      ),
      BagPath("file3.txt") -> VerifiableChecksum(
        md5 = Some(ChecksumValue("aaa333")),
        sha256 = ChecksumValue("bbb333")
      )
    )
  }

  describe("handles errors correctly") {
    it("if multiple manifests have different lists of files") {
      val md5 = asStream("""
          |aaa111   file1.txt
          |aaa222   file2.txt
          |aaa333   file3.txt
          |""".stripMargin)

      val sha256 = asStream("""
          |bbb444   file4.txt
          |bbb555   file5.txt
          |""".stripMargin)

      val result =
        CombinedManifestParser.createFileLists(md5 = Some(md5), sha256 = sha256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage shouldBe "Different manifests refer to different lists of files!"
    }

    it("if a manifest has duplicate entries") {
      val sha256 = asStream("""
          |aaa111   file1.txt
          |aaa111   file1.txt
          |aaa222   file2.txt
          |""".stripMargin)

      val result = CombinedManifestParser.createFileLists(sha256 = sha256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Manifest contains duplicate paths:")
    }

    it("if a manifest has a line which isn't `checksum filepath`") {
      val sha256 = asStream("""
          |notavalidline
          |aaa111   file1.txt
          |""".stripMargin)

      val result = CombinedManifestParser.createFileLists(sha256 = sha256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Failed to parse the following lines:")
    }

    it("if the checksum is not hex-encoded") {
      val sha256 = asStream("""
          |nothex   badfile.txt
          |aaa111   file1.txt
          |""".stripMargin)

      val result = CombinedManifestParser.createFileLists(sha256 = sha256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Failed to parse the following lines:")
    }
  }

  private def asStream(s: String): InputStream =
    stringCodec.toStream(s).right.value
}
