package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.FilterInputStream

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.streaming.Codec._

/** The archive files used in these tests were deliberately created
  * with external tools to get better examples of "real" files,
  * and because they're simpler than doing it with Scala!
  *
  */
class UnarchiverTest extends AnyFunSpec with Matchers with EitherValues {

  /** The package for this test was created with the bash script:
    *
    *     mkdir -p numbers
    *
    *     for i in $(seq 0 100); do
    *       echo "$i" > numbers/"$i"
    *     done
    *
    *     tar -czvf numbers.tar.gz numbers
    *
    */
  it("unpacks a tar.gz file") {
    val inputStream = getClass.getResourceAsStream("/numbers.tar.gz")

    val archiveIterator = Unarchiver.open(inputStream).right.value

    archiveIterator.foreach {
      case (archiveEntry, entryInputStream) =>
        archiveEntry shouldBe a[TarArchiveEntry]

        if (!archiveEntry.isDirectory) {
          val contents = stringCodec.fromStream(entryInputStream).right.value

          val contentsNumber = contents.trim
          val filenameNumber = archiveEntry.getName.split("/").last

          contentsNumber shouldBe filenameNumber
        }
    }
  }

  it("unpacks a tar.gz file when the provided stream does not support mark()") {
    val inputStream = getClass.getResourceAsStream("/numbers.tar.gz")

    val unmarkableStream = new FilterInputStream(inputStream) {
      override def markSupported(): Boolean = false
    }

    Unarchiver.open(unmarkableStream) shouldBe a[Right[_, _]]
  }

  it("handles the caller closing the input stream") {
    val inputStream = getClass.getResourceAsStream("/numbers.tar.gz")

    val archiveIterator = Unarchiver.open(inputStream).right.value

    archiveIterator.foreach {
      case (_, entryInputStream) =>
        entryInputStream.close()
    }
  }

  /** The file for this test was created with the bash script:
    *
    *     echo "hello world" > greeting.txt
    *     gzip greeting.txt
    *
    */
  it("fails if the uncompressed stream is not a container") {
    val inputStream = getClass.getResourceAsStream("/greeting.txt.gz")

    val error = Unarchiver.open(inputStream).left.value

    error shouldBe a[ArchiveFormatError]
    error.e.getMessage should startWith(
      "No Archiver found for the stream signature"
    )
  }

  it("fails if the file is not a compressed stream") {
    val inputStream = stringCodec.toStream("hello world").right.value

    val error = Unarchiver.open(inputStream).left.value

    error shouldBe a[CompressorError]
    error.e.getMessage should startWith(
      "No Compressor found for the stream signature"
    )
  }

  it("fails if the input stream is null") {
    Unarchiver.open(null).left.value shouldBe a[CompressorError]
  }
}
