package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import uk.ac.wellcome.storage.ObjectLocation

class S3Uploader(implicit s3Client: AmazonS3) {

  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  // Set bufferReadLimit
  // To prevent 'com.amazonaws.ResetException: Failed to reset the request input stream' being thrown.
  // (see https://github.com/aws/aws-sdk-java/issues/427)
  // (and https://github.com/wellcometrust/platform/issues/3481)
  //
  // If a transfer fails, the AWS SDK retries by rewinding the input stream to the 'mark' set in the buffer at the start.
  // The 'ReadLimit' determines how far the stream can be rewound, if it is smaller than the bytes sent before an error
  // occurs the mark will be invalid and a ResetException is thrown.
  //
  // When using a BufferedInputStream up to 'ReadLimit' bytes are stored in memory, and this must be (at least one byte)
  // larger than the PUT.  The buffer grows dynamically up to this limit.
  //
  // For multipart PUT requests the size must be larger than each PART.
  //
  // To prevent this exception a constant maximum size is set
  // assuming a maximum file of 600GB PUT as 10,000 multipart requests
  // = 60MB ~ 100MB read limit
  // this is a generous estimate and should be sufficient,
  // also given x10 concurrent streams = 10x100MB = 1GB memory overhead which we are comfortable with.
  // This change was tested to reproduce the error with a proxy that dropped traffic to simulate S3 network failure.
  private val MB: Int = 1024 * 1024
  private val bufferReadLimit: Int = 100 * MB

  def putObject(
    inputStream: InputStream,
    streamLength: Long,
    uploadLocation: ObjectLocation
  ): UploadResult = {
    val metadata = new ObjectMetadata()
    metadata.setContentLength(streamLength)

    val putObjectRequest = new PutObjectRequest(
      uploadLocation.namespace,
      uploadLocation.path,
      inputStream,
      metadata
    )

    putObjectRequest.getRequestClientOptions.setReadLimit(bufferReadLimit)

    val upload: Upload = transferManager
      .upload(putObjectRequest)

    upload.waitForUploadResult()
  }
}
