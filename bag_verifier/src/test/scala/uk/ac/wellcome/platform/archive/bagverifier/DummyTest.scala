package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.FunSpec
import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, VerifiableLocation, VerifiedSuccess, Verifier}

class DummyTest extends FunSpec {

  import java.time.{Instant, ZoneId}

  import uk.ac.wellcome.platform.archive.common.bagit.models._
  import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, BagManifest}
  import uk.ac.wellcome.storage.ObjectLocation

  val checksumAlgorithm = ChecksumAlgorithm("BBQ95")
  val root = ObjectLocation("root","location")
  val checksumValue = ChecksumValue("checksum")
  val itemPath = BagPath("path")

  val fileManifest = BagManifest(
    checksumAlgorithm,
    List(BagFile(checksumValue, itemPath))
  )
  val tagManifest = BagManifest(
    checksumAlgorithm,
    List(BagFile(checksumValue, itemPath))
  )

  val bagInfo = BagInfo(
    ExternalIdentifier("id"),
    PayloadOxum(0L, 1),
    Instant.now().atZone(ZoneId.of("UTC")).toLocalDate
  )

  val bag = Bag(bagInfo, fileManifest, tagManifest)

  it("fails") {

    import uk.ac.wellcome.platform.archive.common.bagit.models._

    implicit val objectLocationVerifier = new DummyObjectLocationVerifier()

    import uk.ac.wellcome.platform.archive.common.verify.Verifiable._
    import uk.ac.wellcome.platform.archive.common.verify.Verification._

    // verifications

    val verified = bag.verify(root)

    println(verified)
  }
}

class DummyObjectLocationVerifier extends Verifier {
  override def verify(location: VerifiableLocation) = VerifiedSuccess(location)
}
