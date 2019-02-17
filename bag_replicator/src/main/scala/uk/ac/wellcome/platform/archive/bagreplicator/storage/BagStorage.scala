package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

import scala.concurrent.{ExecutionContext, Future}

class BagStorage(s3Client: AmazonS3)(implicit ec: ExecutionContext)
    extends Logging {

  val s3PrefixCopier = new S3PrefixCopier(s3Client)

  def duplicateBag(
    sourceBagLocation: BagLocation,
    storageDestination: ReplicatorDestinationConfig
  ): Future[BagLocation] = {
    debug(s"duplicating bag from $sourceBagLocation to $storageDestination")

    val dstBagLocation = sourceBagLocation.copy(
      storageNamespace = storageDestination.namespace,
      storagePrefix = storageDestination.rootPath
    )

    val future = s3PrefixCopier.copyObjects(
      srcLocationPrefix = sourceBagLocation.objectLocation,
      dstLocationPrefix = dstBagLocation.objectLocation
    )

    future.map { _ => dstBagLocation }
  }
}
