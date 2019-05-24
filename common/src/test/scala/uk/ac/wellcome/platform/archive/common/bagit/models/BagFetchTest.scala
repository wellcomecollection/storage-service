package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream
import java.net.URI

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}

class BagFetchTest extends FunSpec with Matchers {

  describe("write") {
    it("writes the lines of a fetch.txt") {
      val entries = Seq(
        createBagFetchEntryWith(
          uri = "http://example.org/",
          length = 25,
          path = "example.txt"
        ),
        createBagFetchEntryWith(
          uri = "https://wellcome.ac.uk/",
          path = "logo.png"
        )
      )

      val expected =
        s"""
           |http://example.org/ 25 example.txt
           |https://wellcome.ac.uk/ - logo.png
       """.stripMargin.trim

      BagFetch.write(entries) shouldBe expected
    }

    it("percent-encodes a CR, LF or CRLF in the filename") {
      val entries = Seq(
        "example\rnumber\r1.txt",
        "example\nnumber\n2.txt",
        "example\r\nnumber\r\n3.txt").map { path =>
        createBagFetchEntryWith(
          uri = "http://example.org/",
          path = path
        )
      }

      Seq(
        createBagFetchEntryWith(
          uri = "http://example.org/",
          path = "example.txt"
        ),
        createBagFetchEntryWith(
          uri = "https://wellcome.ac.uk/",
          path = "logo.png"
        )
      )

      val expected =
        s"""
           |http://example.org/ - example%0Dnumber%0D1.txt
           |http://example.org/ - example%0Anumber%0A2.txt
           |http://example.org/ - example%0D%0Anumber%0D%0A3.txt
       """.stripMargin.trim

      BagFetch.write(entries) shouldBe expected
    }
  }

  describe("read") {
    it("reads the contents of a fetch.txt") {
      val contents = toInputStream(s"""
                                      |http://example.org/\t25 example.txt
                                      |https://wellcome.ac.uk/ -\tlogo.png
       """.stripMargin)

      val expected = Seq(
        createBagFetchEntryWith(
          uri = "http://example.org/",
          length = 25,
          path = "example.txt"
        ),
        createBagFetchEntryWith(
          uri = "https://wellcome.ac.uk/",
          path = "logo.png"
        )
      )

      BagFetch.create(contents).get.files shouldBe expected
    }

    it("handles an empty line in the fetch.txt") {
      val contents = toInputStream(s"""
                                      |http://example.org/\t25 example.txt
                                      |
           |https://wellcome.ac.uk/ -\tlogo.png
       """.stripMargin)

      val expected = Seq(
        createBagFetchEntryWith(
          uri = "http://example.org/",
          length = 25,
          path = "example.txt"
        ),
        createBagFetchEntryWith(
          uri = "https://wellcome.ac.uk/",
          path = "logo.png"
        )
      )

      BagFetch.create(contents).get.files shouldBe expected
    }

    it("handles a file whose size is >Int.MaxValue") {
      val contents = toInputStream(s"""
           |http://example.org/ ${Int.MaxValue}0 example.txt
       """.stripMargin)

      val expected = Seq(
        createBagFetchEntryWith(
          uri = "http://example.org/",
          length = Int.MaxValue.toLong * 10,
          path = "example.txt"
        )
      )

      BagFetch.create(contents).get.files shouldBe expected
    }

    it("correctly decodes a percent-encoded CR/LF/CRLF in the file path") {
      val contents = toInputStream(s"""
                                      |http://example.org/abc - example%0D1%0D.txt
                                      |http://example.org/abc - example%0A2%0A.txt
                                      |http://example.org/abc - example%0D%0A3%0D%0A.txt
       """.stripMargin)

      BagFetch.create(contents).get.files.map { _.path.toString } shouldBe Seq(
        "example\r1\r.txt",
        "example\n2\n.txt",
        "example\r\n3\r\n.txt"
      )
    }

    it("throws an exception if a line is incorrectly formatted") {
      val contents = toInputStream(s"""
                                      |http://example.org/abc - example1.txt
                                      |NO NO NO
                                      |http://example.org/abc - example3.txt
       """.stripMargin)

      val exc = BagFetch.create(contents).failed.get
      exc shouldBe a[RuntimeException]
      exc.getMessage shouldBe "Line <<NO NO NO>> is incorrectly formatted!"
    }
  }

  def createBagFetchEntryWith(uri: String, path: String) =
    BagFetchEntry(
      uri = new URI(uri),
      length = None,
      path = BagPath(path)
    )

  def createBagFetchEntryWith(uri: String, length: Long, path: String) =
    BagFetchEntry(
      uri = new URI(uri),
      length = Some(length),
      path = BagPath(path)
    )

  private def toInputStream(s: String): InputStream =
    IOUtils.toInputStream(s.trim, "UTF-8")
}
