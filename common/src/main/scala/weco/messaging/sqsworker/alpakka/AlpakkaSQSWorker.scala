package weco.messaging.sqsworker.alpakka

import java.net.SocketTimeoutException

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsSource}
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message => SQSMessage}
import weco.json.JsonUtil.fromJson
import weco.messaging.sns.NotificationMessage
import weco.messaging.worker.models.Result
import weco.messaging.worker.AkkaWorker
import weco.monitoring.Metrics

import scala.concurrent.Future

/**
  * Implementation of [[AkkaWorker]] that uses SQS as source and sink.
  * It receives messages from SQS and deletes messages from SQS on successful completion
  */
class AlpakkaSQSWorker[Work, Summary](
  config: AlpakkaSQSWorkerConfig
)(
  val doWork: Work => Future[Result[Summary]]
)(implicit
  val as: ActorSystem,
  val wd: Decoder[Work],
  sc: SqsAsyncClient,
  val metrics: Metrics[Future])
    extends AkkaWorker[SQSMessage, Work, Summary, MessageAction]
    with Logging {
  override protected val metricsNamespace: String =
    config.metricsConfig.namespace

  val parseMessage = (message: SQSMessage) =>
    for {
      notification <- fromJson[NotificationMessage](message.body())
      work <- fromJson[Work](notification.body)
    } yield work

  type SQSAction = SQSMessage => MessageAction

  val parallelism: Int = config.sqsConfig.parallelism
  val source = SqsSource(config.sqsConfig.queueUrl)
  val sink = SqsAckSink.grouped(config.sqsConfig.queueUrl)

  val retryAction: SQSAction = (message: SQSMessage) =>
    MessageAction
      .changeMessageVisibility(message, visibilityTimeout = 0)

  val successfulAction: SQSAction = (message: SQSMessage) =>
    MessageAction.delete(message)

  // We're deliberately quite conservative here -- if a message fails for
  // any reason, we log it (including hte full body) and delete it from the
  // original queue.  This will flag it for human inspection/intervention.
  //
  // This class is used in the storage service, where we want to be careful
  // when things go wrong.  When there is an unexpected failure, we want to
  // give up immediately and wait for human intervention, rather than plough
  // on and make things worse.
  //
  // Ideally we'd put the message on a DLQ, where it could be easily redriven --
  // but there's no good way to put a received message on a DLQ.  We could send
  // to the queue manually, but that introduces other possible failure modes and
  // would need us to update IAM permissions on all our services.
  //
  val failureAction: SQSAction = (message: SQSMessage) => {

    // Note: this log includes the full body of the message, so if something goes wrong
    // further down, we can get it later.
    warn(s"Deleting failed message $message")

    MessageAction.delete(message)
  }

  // We should retry an unhandled failure if it looks like an AWS service
  // flaking out, not a bug in our code.
  //
  // This includes network timeouts, 500 errors, DNS lookup failures.
  // This will cause TerminalFailures with one of these errors to be sent
  // back to the SQS queue for another try.
  //
  // We should be quite conservative here -- as above, we generally want
  // to hard fail when something goes wrong, but we have occasionally seen
  // AWS services flake out on us in a way that isn't caught by our other
  // retrying logic, e.g. https://github.com/wellcomecollection/platform/issues/5419
  //
  override def isRetryable(t: Throwable): Boolean =
    t match {
      case exc: AwsServiceException if exc.statusCode() == 500 =>
        true

      case exc: SdkClientException
          if exc.getMessage.startsWith("Unable to execute HTTP request") =>
        true

      case exc: SdkClientException
          if exc.getMessage.startsWith(
            "Received an UnknownHostException when attempting to interact with a service") =>
        true

      case _: SocketTimeoutException => true

      case t => super.isRetryable(t)
    }
}
