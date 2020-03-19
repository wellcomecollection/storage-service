package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, MD5, SHA256}
import uk.ac.wellcome.storage.streaming.Codec._

class BagManifestTest extends FunSpec with Matchers with EitherValues with TryValues {
  it("parses a valid manifest") {
    val inputStream = toInputStream("""
      |abc123   file1.txt
      |def456   file2.txt
      |""".stripMargin)

    val manifest = BagManifest.create(inputStream, algorithm = SHA256)

    manifest.success.value shouldBe BagManifest(
      checksumAlgorithm = SHA256,
      entries = Map(
        BagPath("file1.txt") -> Checksum(algorithm = SHA256, value = ChecksumValue("abc123")),
        BagPath("file2.txt") -> Checksum(algorithm = SHA256, value = ChecksumValue("def456")),
      )
    )
  }

  it("sets the checksum algorithm correctly") {
    val inputStream = toInputStream("""
                                      |abc123   file1.txt
                                      |def456   file2.txt
                                      |""".stripMargin)

    val manifest = BagManifest.create(inputStream, algorithm = MD5)

    manifest.success.value shouldBe BagManifest(
      checksumAlgorithm = MD5,
      entries = Map(
        BagPath("file1.txt") -> Checksum(algorithm = MD5, value = ChecksumValue("abc123")),
        BagPath("file2.txt") -> Checksum(algorithm = MD5, value = ChecksumValue("def456"))
      )
    )
  }

  describe("catching errors in the manifest file") {
    it("spots duplicate entries") {
      val inputStream = toInputStream("""
                              |aaa111   file1.txt
                              |aaa111   file1.txt
                              |aaa222   file2.txt
                              |""".stripMargin)

      val result = BagManifest.create(inputStream, algorithm = SHA256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Manifest contains duplicate paths:")
    }

    it("if a manifest has a line which isn't `checksum filepath`") {
      val inputStream = toInputStream("""
                              |notavalidline
                              |aaa111   file1.txt
                              |""".stripMargin)

      val result = BagManifest.create(inputStream, algorithm = SHA256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Failed to parse the following lines:")
    }

    it("if the checksum is not hex-encoded") {
      val inputStream = toInputStream("""
                              |nothex   badfile.txt
                              |aaa111   file1.txt
                              |""".stripMargin)

      val result = BagManifest.create(inputStream, algorithm = SHA256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Failed to parse the following lines:")
    }

    it("if the manifest does not contain any valid entries") {
      val inputStream = toInputStream("neroifuighsdfiug")

      val result = BagManifest.create(inputStream, algorithm = SHA256)

      val err = result.failure.exception

      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Failed to parse the following lines:")
    }
  }

  private def toInputStream(s: String): InputStream =
    stringCodec.toStream(s).right.value
}
