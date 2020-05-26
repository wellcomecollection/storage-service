package uk.ac.wellcome.platform.archive.indexer.bag.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestGenerators,
  StorageManifestGenerators
}

class FileSuffixCounterTest
    extends AnyFunSpec
    with Matchers
    with IngestGenerators
    with StorageManifestGenerators {

  val files = List(
    createStorageManifestFileWith(
      name = "foo.txt"
    ),
    createStorageManifestFileWith(
      name = "bar.txt"
    ),
    createStorageManifestFileWith(
      name = "bat.png"
    ),
    createStorageManifestFileWith(
      name = "mysteryfile"
    ),
    createStorageManifestFileWith(
      name = "bad..bmp"
    ),
    createStorageManifestFileWith(
      name = "worse.jif."
    ),
    createStorageManifestFileWith(
      name = "file.with.full.stops.scala"
    )
  )

  it("works") {
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
