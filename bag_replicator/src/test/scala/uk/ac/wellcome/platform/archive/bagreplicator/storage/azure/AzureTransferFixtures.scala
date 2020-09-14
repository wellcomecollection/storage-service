package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import com.amazonaws.services.s3.model.S3ObjectSummary
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.s3.S3ObjectLocation

trait AzureTransferFixtures extends StorageRandomThings {
  def createS3ObjectSummaryFrom(
    location: S3ObjectLocation,
    size: Long = randomInt(from = 1, to = 50)
  ): S3ObjectSummary = {
    val summary = new S3ObjectSummary()
    summary.setBucketName(location.bucket)
    summary.setKey(location.key)

    // By default, the size of an S3ObjectSummary() is zero.  We don't want it to
    // be zero, because then the underlying S3 SDK will skip trying to read it;
    // the correct size will be set in the StreamStore[S3ObjectSummary].
    summary.setSize(size)

    summary
  }
}
