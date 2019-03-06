package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.collection.JavaConverters._

trait BagReplicatorFixtures
    extends S3
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures {

  def verifyBagCopied(src: BagLocation, dst: BagLocation): Assertion = {
    val sourceItems = getObjectSummaries(src)
    val sourceKeyEtags = sourceItems.map { _.getETag }

    val destinationItems = getObjectSummaries(dst)
    val destinationKeyEtags = destinationItems.map { _.getETag }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }

  private def getObjectSummaries(
    bagLocation: BagLocation): List[S3ObjectSummary] =
    s3Client
      .listObjects(bagLocation.storageNamespace, bagLocation.completePath)
      .getObjectSummaries
      .asScala
      .toList

}
