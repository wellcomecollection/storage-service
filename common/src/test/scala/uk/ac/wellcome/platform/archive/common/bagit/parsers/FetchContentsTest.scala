package uk.ac.wellcome.platform.archive.common.bagit.parsers

import java.io.InputStream
import java.net.URL

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.FetchEntry

class FetchContentsTest extends FunSpec with Matchers {

  describe("write") {
    it("writes the lines of a fetch.txt") {
      val entries = Seq(
        FetchEntry(
          url = new URL("http://example.org/"),
          length = Some(25),
          filepath = "example.txt"),
        FetchEntry(
          url = new URL("https://wellcome.ac.uk/"),
          length = None,
          filepath = "logo.png")
      )

      val expected =
        s"""
           |http://example.org/ 25 example.txt
           |https://wellcome.ac.uk/ - logo.png
       """.stripMargin.trim

      FetchContents.write(entries) shouldBe expected
    }

    it("percent-encodes a CR, LF or CRLF in the filename") {
      val entries = Seq(
        "example\rnumber\r1.txt",
        "example\nnumber\n2.txt",
        "example\r\nnumber\r\n3.txt").map { filepath =>
        FetchEntry(
          url = new URL("http://example.org/"),
          length = None,
          filepath = filepath)
      }

      Seq(
        FetchEntry(
          url = new URL("http://example.org/"),
          length = None,
          filepath = "example.txt"),
        FetchEntry(
          url = new URL("https://wellcome.ac.uk/"),
          length = None,
          filepath = "logo.png")
      )

      val expected =
        s"""
           |http://example.org/ - example%0Dnumber%0D1.txt
           |http://example.org/ - example%0Anumber%0A2.txt
           |http://example.org/ - example%0D%0Anumber%0D%0A3.txt
       """.stripMargin.trim

      FetchContents.write(entries) shouldBe expected
    }

    it("percent encodes whitespace characters in the URL") {
      val entries =
        Seq("http://example.org/x y z", "http://example.org/a\tb\tc").map {
          url =>
            FetchEntry(
              url = new URL(url),
              length = None,
              filepath = "example.txt"
            )
        }

      val expected =
        s"""
           |http://example.org/x%20y%20z - example.txt
           |http://example.org/a%09b%09c - example.txt
       """.stripMargin.trim

      FetchContents.write(entries) shouldBe expected
    }
  }

  describe("read") {
    it("reads the contents of a fetch.txt") {
      val contents = toInputStream(s"""
           |http://example.org/\t25 example.txt
           |https://wellcome.ac.uk/ -\tlogo.png
       """.stripMargin)

      val expected = Seq(
        FetchEntry(
          url = new URL("http://example.org/"),
          length = Some(25),
          filepath = "example.txt"),
        FetchEntry(
          url = new URL("https://wellcome.ac.uk/"),
          length = None,
          filepath = "logo.png")
      )

      FetchContents.read(contents).get shouldBe expected
    }

    it("handles an empty line in the fetch.txt") {
      val contents = toInputStream(s"""
           |http://example.org/\t25 example.txt
           |
           |https://wellcome.ac.uk/ -\tlogo.png
       """.stripMargin)

      val expected = Seq(
        FetchEntry(
          url = new URL("http://example.org/"),
          length = Some(25),
          filepath = "example.txt"),
        FetchEntry(
          url = new URL("https://wellcome.ac.uk/"),
          length = None,
          filepath = "logo.png")
      )

      FetchContents.read(contents).get shouldBe expected
    }

    it("decodes a percent-encoded CR/LF/CRLF in the file path") {
      val contents = toInputStream(s"""
           |http://example.org/abc - example%0D1%0D.txt
           |http://example.org/abc - example%0A2%0A.txt
           |http://example.org/abc - example%0D%0A3%0D%0A.txt
       """.stripMargin)

      FetchContents.read(contents).get.map { _.filepath } shouldBe Seq(
        "example\r1\r.txt",
        "example\n2\n.txt",
        "example\r\n3\r\n.txt"
      )
    }

    it("decodes a percent-encoded whitespace character in the URL") {
      val contents = toInputStream(s"""
           |http://example.org/x%20y%20z - example.txt
           |http://example.org/a%09b%09c - example.txt
       """.stripMargin)

      FetchContents.read(contents).get.map { _.url.toString } shouldBe Seq(
        "http://example.org/x y z",
        "http://example.org/a\tb\tc",
      )
    }

    it("throws an exception if a line is incorrectly formatted") {
      val contents = toInputStream(s"""
           |http://example.org/abc - example1.txt
           |NO NO NO
           |http://example.org/abc - example3.txt
       """.stripMargin)

      val exc = FetchContents.read(contents).failed.get
      exc shouldBe a[RuntimeException]
      exc.getMessage shouldBe "Line <<NO NO NO>> is incorrectly formatted!"
    }
  }

  private def toInputStream(s: String): InputStream =
    IOUtils.toInputStream(s.trim, "UTF-8")
}
