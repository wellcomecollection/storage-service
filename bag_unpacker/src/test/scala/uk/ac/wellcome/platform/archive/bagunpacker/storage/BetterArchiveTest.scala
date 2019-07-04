package uk.ac.wellcome.platform.archive.bagunpacker.storage

import org.apache.commons.compress.archivers.ArchiveException
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.compressors.CompressorException
import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.storage.streaming.Codec._

/** The archive files used in these tests were deliberately created
  * with external tools to get better examples of "real" files,
  * and because they're simpler than doing it with Scala!
  *
  */
class BetterArchiveTest extends FunSpec with Matchers with EitherValues with TryValues {

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

    val archiveIterator = BetterArchive.unpack(inputStream).success.value

    archiveIterator.foreach { case (archiveEntry, entryInputStream) =>
      archiveEntry shouldBe a[TarArchiveEntry]

      if (!archiveEntry.isDirectory) {
        val contents = stringCodec.fromStream(entryInputStream).right.value

        val contentsNumber = contents.trim
        val filenameNumber = archiveEntry.getName.split("/").last

        contentsNumber shouldBe filenameNumber
      }
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

    val error = BetterArchive.unpack(inputStream).failure

    error.exception shouldBe a[ArchiveException]
    error.exception.getMessage should startWith("No Archiver found for the stream signature")
  }

  it("fails if the file is not a compressed stream") {
    val inputStream = stringCodec.toStream("hello world").right.value

    val error = BetterArchive.unpack(inputStream).failure

    error.exception shouldBe a[CompressorException]
    error.exception.getMessage should startWith("No Compressor found for the stream signature")
  }
}
