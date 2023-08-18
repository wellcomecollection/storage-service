package weco.messaging.worker.models

sealed trait Result[Summary] {
  val summary: Option[Summary]

  def name: String =
    this.getClass.getSimpleName

  def pretty: String =
    s"$name: ${summary.getOrElse("<no-summary>")}"
}

// An operation that failed, and which the caller thinks can't be retried.
//
// e.g. trying to write an object to a non-existent S3 bucket.  This will always
// fail, no matter how often we try.
case class TerminalFailure[Summary](
  failure: Throwable,
  summary: Option[Summary] = Option.empty[Summary]
) extends Result[Summary]

// An operation that failed, and which the caller knows can be retried.
//
// e.g. a 500 error when trying to write an object to S3.  This will usually
// succeed if we try again after a short pause.
case class RetryableFailure[Summary](
  failure: Throwable,
  summary: Option[Summary] = Option.empty[Summary]
) extends Result[Summary]

// An operation that succeeded.
case class Successful[Summary](
  summary: Option[Summary] = Option.empty[Summary]
) extends Result[Summary]
