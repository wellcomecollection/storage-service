package weco.storage_service.bag_tracker.client

import org.apache.pekko.stream.StreamTcpException

trait TrackerClientBase {
  def isRetryable(err: Throwable): Boolean =
    err match {
      // This error can occur if the tracker API is unavailable.  Tasks should wait
      // and then retry the request, if possible.
      // See https://github.com/wellcomecollection/platform/issues/4834
      case _: StreamTcpException => true

      case _ => false
    }
}
