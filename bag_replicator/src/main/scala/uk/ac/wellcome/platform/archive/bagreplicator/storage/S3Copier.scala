package uk.ac.wellcome.platform.archive.bagreplicator.storage

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

class S3Copier(s3Client: AmazonS3) extends Logging with ObjectCopier {

  import com.amazonaws.services.s3.transfer.TransferManagerBuilder

  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  def copy(src: ObjectLocation, dst: ObjectLocation): Unit = {
    debug(s"Copying ${s3Uri(src)} -> ${s3Uri(dst)}")

    val copyTransfer = transferManager.copy(
      src.namespace,
      src.key,
      dst.namespace,
      dst.key
    )

    copyTransfer.waitForCopyResult()

    ()
  }

  private def s3Uri(objectLocation: ObjectLocation): String =
    s"s3://${objectLocation.namespace}/${objectLocation.key}"
}
