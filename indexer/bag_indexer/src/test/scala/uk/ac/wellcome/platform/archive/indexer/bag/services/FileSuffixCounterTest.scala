package uk.ac.wellcome.platform.archive.indexer.bag.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.indexer.bags.services.FileSuffixCounter

class FileSuffixCounterTest
    extends AnyFunSpec
    with Matchers
    with IngestGenerators
    with StorageManifestGenerators {

  private val names = Seq(
    "foo.TXT",
    "bar.txt",
    "bat.png",
    "mystery file",
    "bad..bmp",
    "worse.jif.",
    "file.with.full.stops.scala"
  )

  private val files = names.map { name =>
    createStorageManifestFileWith(name = name)
  }

  it("tallies the file suffixes correctly") {
    val suffixMap = FileSuffixCounter.count(files)

    suffixMap shouldBe Map(
      "png" -> 1,
      "bmp" -> 1,
      "txt" -> 2,
      "jif" -> 1,
      "scala" -> 1
    )
  }
}
