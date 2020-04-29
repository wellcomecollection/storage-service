package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.FetchMetadataGenerators

class BagFetchTest extends AnyFunSpec with Matchers with FetchMetadataGenerators {
  describe("read") {
    it("reads the contents of a fetch.txt") {
      val contents = toInputStream(s"""
                                      |http://example.org/\t25 data/example.txt
                                      |https://wellcome.ac.uk/ -\tdata/logo.png
       """.stripMargin)

      val expected = Map(
        BagPath("data/example.txt") -> createFetchMetadataWith(
          uri = "http://example.org/",
          length = 25
        ),
        BagPath("data/logo.png") -> createFetchMetadataWith(
          uri = "https://wellcome.ac.uk/",
          length = None
        )
      )

      BagFetch.create(contents).get.entries shouldBe expected
    }

    it("handles an empty line in the fetch.txt") {
      val contents = toInputStream(s"""
           |http://example.org/\t25 data/example.txt
           |
           |https://wellcome.ac.uk/ -\tdata/logo.png
       """.stripMargin)

      val expected = Map(
        BagPath("data/example.txt") -> createFetchMetadataWith(
          uri = "http://example.org/",
          length = 25
        ),
        BagPath("data/logo.png") -> createFetchMetadataWith(
          uri = "https://wellcome.ac.uk/",
          length = None
        )
      )

      BagFetch.create(contents).get.entries shouldBe expected
    }

    it("handles a file whose size is >Int.MaxValue") {
      val contents = toInputStream(s"""
           |http://example.org/ ${Int.MaxValue}0 data/example.txt
       """.stripMargin)

      val expected = Map(
        BagPath("data/example.txt") -> createFetchMetadataWith(
          uri = "http://example.org/",
          length = Int.MaxValue.toLong * 10
        )
      )

      BagFetch.create(contents).get.entries shouldBe expected
    }

    it("correctly decodes a percent-encoded CR/LF/CRLF in the file path") {
      val contents = toInputStream(s"""
                                      |http://example.org/abc - data/example%0D1%0D.txt
                                      |http://example.org/abc - data/example%0A2%0A.txt
                                      |http://example.org/abc - data/example%0D%0A3%0D%0A.txt
       """.stripMargin)

      BagFetch.create(contents).get.paths.map { _.toString } shouldBe Seq(
        "data/example\r1\r.txt",
        "data/example\n2\n.txt",
        "data/example\r\n3\r\n.txt"
      )
    }

    it("throws an exception if a line is incorrectly formatted") {
      val contents = toInputStream(s"""
                                      |http://example.org/abc - data/example1.txt
                                      |NO NO NO
                                      |http://example.org/abc - data/example3.txt
       """.stripMargin)

      val exc = BagFetch.create(contents).failed.get
      exc shouldBe a[RuntimeException]
      exc.getMessage shouldBe "Line <<NO NO NO>> is incorrectly formatted!"
    }

    it("throws an exception if a fetch.txt contains a tag file") {
      val contents = toInputStream(s"""
        |http://example.org/file1 - data/example1.txt
        |http://example.org/file2 - data/example2.txt
        |http://example.org/file3 - manifest-sha256.txt
        |http://example.org/file3 - tagmanifest-sha256.txt
       """.stripMargin)

      val exc = BagFetch.create(contents).failed.get
      exc shouldBe a[RuntimeException]
      exc.getMessage shouldBe "fetch.txt should not contain tag files: manifest-sha256.txt, tagmanifest-sha256.txt"
    }

    it("throws an exception if a fetch.txt contains duplicate paths") {
      val contents = toInputStream(s"""
        |http://example.org/file1 - data/example1.txt
        |http://example.org/file1 - data/example1.txt
        |http://example.org/file2 - data/example2.txt
        |http://example.org/file2 - data/example2.txt
        |http://example.org/file3 - data/example3.txt
       """.stripMargin)

      val exc = BagFetch.create(contents).failed.get
      exc shouldBe a[RuntimeException]
      exc.getMessage shouldBe "fetch.txt contains duplicate paths: data/example1.txt, data/example2.txt"
    }
  }

  private def toInputStream(s: String): InputStream =
    IOUtils.toInputStream(s.trim, "UTF-8")
}
