package uk.ac.wellcome.platform.archive.common.storage.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3RangedReader(implicit s3Client: AmazonS3) extends Logging {
  def getBytes(
    location: S3ObjectLocation,
    offset: Long,
    count: Long,
    totalLength: Long
  ): Array[Byte] = {
    if (offset >= totalLength)
      throw new IllegalArgumentException(s"Offset is after the end of the object: $offset >= $totalLength")

    val getRequest =
      new GetObjectRequest(location.bucket, location.key)

    // The S3 Range request is *inclusive* of the boundaries.
    //
    // For example, if you read (start=0, end=5), you get bytes [0, 1, 2, 3, 4, 5].
    // We never want to read more than bufferSize bytes at a time.
    //
    val requestWithRange =
      if (offset + count >= totalLength || isNull(count)) {
        debug(s"Reading $location: $offset-[end] / $totalLength")
        getRequest.withRange(offset)
      } else {
        val endRange = offset + count - 1
        assert(endRange >= offset, s"End of range is greater than offset: $endRange >= $offset")
        debug(s"Reading $location: $offset-$endRange / $totalLength")
        getRequest.withRange(offset, endRange)
      }

    // Remember to close the input stream afterwards, or we get errors like
    //
    //    com.amazonaws.SdkClientException: Unable to execute HTTP request:
    //    Timeout waiting for connection from pool
    //
    val s3InputStream = s3Client.getObject(requestWithRange).getObjectContent
    val byteArray = IOUtils.toByteArray(s3InputStream)
    s3InputStream.close()

    byteArray
  }

  // If `count` is null, we read to the end.  This is for interop with the BlobRange
  // class for specifying ranges in Azure objects.
  private def isNull(count: Long): Boolean =
    Option(count) == Some(0)
}
