package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.ReplicatedBagVerifyContext
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.S3BagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, ReplicatedBagVerifier}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

trait AzureBagVerifier extends BagVerifier[ReplicatedBagVerifier[S3ObjectLocationPrefix,S3ObjectLocationPrefix,AzureBlobLocation, AzureBlobLocationPrefix], AzureBlobLocation, AzureBlobLocationPrefix]

class AzureReplicatedBagVerifier(val primaryBucket: String)(
  implicit val s3Client: AmazonS3
) extends ReplicatedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix,AzureBlobLocation, AzureBlobLocationPrefix]
  with AzureBagVerifier{

  override val srcStreamStore: StreamStore[S3ObjectLocation] =
    new S3StreamStore()
  //override val replicaStreamStore: StreamStore[AzureBlobLocation] = new S3StreamStore()
  override implicit val bagReader: BagReader[S3ObjectLocation, AzureBlobLocationPrefix] = _
  override implicit val listing: Listing[AzureBlobLocationPrefix, S3ObjectLocation] = _
  override implicit val resolvable: Resolvable[S3ObjectLocation] = _
  override implicit val fixityChecker: FixityChecker[S3ObjectLocation] = _

  override def createPrefix(path: String): AzureBlobLocationPrefix = ???

  override def getRelativePath(root: AzureBlobLocationPrefix, location: S3ObjectLocation): String = ???

  override val replicaStreamStore: StreamStore[AzureBlobLocation] = _
}