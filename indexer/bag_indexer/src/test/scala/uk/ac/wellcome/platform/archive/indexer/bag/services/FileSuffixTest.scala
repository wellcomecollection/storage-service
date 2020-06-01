package uk.ac.wellcome.platform.archive.indexer.bag.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.indexer.bags.services.FileSuffix

class FileSuffixTest
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

  it("extracts the file suffixes correctly") {
    val suffixes = names.map(FileSuffix.getSuffix)

    suffixes shouldBe Seq(
      "png",
      "bmp",
      "txt",
      "jif",
      "scala"
    )
  }
}
