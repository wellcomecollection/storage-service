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

  private val names = Map(
    "foo.JAR" -> Some("jar"),
    "bar.txt" -> Some("txt"),
    "bat.png" -> Some("png"),
    "a.b.log" -> Some("log"),
    "no..bmp" -> Some("bmp"),
    "ab.jif." -> None,
    ".abcdef" -> None,
    "sp aces" -> None,
    "without" -> None,
    "" -> None
  )

  it("extracts the file suffixes correctly") {
    val suffixes = names.keys.toList.map(FileSuffix.getSuffix)

    suffixes shouldBe names.values.toList
  }
}
