package weco.messaging.worker

import grizzled.slf4j.Logging
import weco.messaging.worker.models._
import weco.monitoring.Metrics

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Worker[Message, Work, Summary, Action] extends Logging {
  protected val parseMessage: Message => Try[Work]
  protected val doWork: Work => Future[Result[Summary]]

  protected val successfulAction: Message => Action
  protected val retryAction: Message => Action
  protected val failureAction: Message => Action

  implicit val metrics: Metrics[Future]
  protected val metricsNamespace: String

  implicit val ec: ExecutionContext

  protected def isRetryable(t: Throwable): Boolean =
    false

  def process(message: Message): Future[Action] = {
    val startTime = Instant.now()

    for {
      result <- parseMessage(message) match {
        case Failure(e) => Future.successful(TerminalFailure[Summary](e))

        case Success(work) =>
          doWork(work) recover {
            case e => TerminalFailure[Summary](e)
          }
      }

      _ = log(result)
      _ <- recordEnd(startTime = startTime, result = result)

      action = chooseAction(result)
    } yield action(message)
  }

  private def chooseAction(result: Result[_]): Message => Action =
    result match {
      case _: Successful[_]       => successfulAction
      case _: RetryableFailure[_] => retryAction

      // Although in general terminal failures mean a message should be
      // stopped immediately, we do allow overriding that here in certain
      // cases.  This allows us to handle certain classes of flaky errors
      // (e.g. AWS networking issues) in one place, rather than adding code
      // to recognise them as Retryable everywhere in the stack.
      case TerminalFailure(t, _) if isRetryable(t) =>
        retryAction

      case _: TerminalFailure[_] => failureAction
    }

  private def log(result: Result[_]): Unit =
    result match {
      case r @ Successful(_)          => info(r.pretty)
      case r @ RetryableFailure(e, _) => warn(r.pretty, e)
      case r @ TerminalFailure(e, _)  => error(r.toString, e)
    }

  /** Records metrics about the work that's just been completed; in particular the
    * outcome and the duration.
    */
  private def recordEnd(startTime: Instant, result: Result[_]): Future[Unit] = {
    val futures = Seq(
      metrics.incrementCount(s"$metricsNamespace/${result.name}"),
      metrics
        .recordValue(s"$metricsNamespace/Duration", secondsSince(startTime))
    )

    Future
      .sequence(futures)
      .map(_ => ())
      .recover {
        case e =>
          warn(s"Unable to record metrics: $e")
          ()
      }
  }

  private def secondsSince(startTime: Instant): Long =
    Duration
      .between(startTime, Instant.now())
      .getSeconds
}
