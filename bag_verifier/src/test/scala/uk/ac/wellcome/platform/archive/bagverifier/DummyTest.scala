package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.FunSpec
import uk.ac.wellcome.platform.archive.common.verify.Verifiable._

class DummyTest extends FunSpec {

  import java.time.{Instant, ZoneId}

  import uk.ac.wellcome.platform.archive.common.bagit.models._
  import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, FileManifest}
  import uk.ac.wellcome.storage.ObjectLocation

  val checksumAlgorithm = ChecksumAlgorithm("BBQ95")
  val root = ObjectLocation("root","location")
  val fileManifest = FileManifest(
    checksumAlgorithm,
    List(BagDigestFile("checksum", BagItemPath("path")))
  )
  val tagManifest = FileManifest(
    checksumAlgorithm,
    List(BagDigestFile("checksum", BagItemPath("path")))
  )
  val bagInfo = BagInfo(
    ExternalIdentifier("id"),
    PayloadOxum(0L, 1),
    Instant.now().atZone(ZoneId.of("UTC")).toLocalDate
  )

  val bag = Bag(bagInfo, fileManifest, tagManifest)

  it("fails") {
    val verifiable = bag.verifiable

    println(verifiable)
  }
}
