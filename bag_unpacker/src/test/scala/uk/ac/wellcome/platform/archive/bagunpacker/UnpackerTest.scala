package uk.ac.wellcome.platform.archive.bagunpacker

import java.io.{File}

import org.apache.commons.compress.archivers.ArchiveEntry
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.{RandomThings, ZipBagItFixture}
//import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import java.io.FileInputStream

class UnpackerTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with S3
    with ZipBagItFixture
    with IntegrationPatience {

  it("unpacks") {
    withLocalS3Bucket { _ =>
        val file = new File(
          "/Users/k/Desktop/tar-archive-name.tar.gz"
        )

        val inputStream = new FileInputStream(file)

        val entryStreamEither = Unpacker(inputStream)

      entryStreamEither shouldBe a[Right[_,_]]
      val entryStream = entryStreamEither.right.get

        val entries = entryStream.map(
          entry => (entry.getName, entry.getSize)
        )

        entries shouldBe Nil
      }
    }
}

