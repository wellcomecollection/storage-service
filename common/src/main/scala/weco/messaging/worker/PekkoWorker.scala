package weco.messaging.worker

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.Future

trait PekkoWorker[Message, Work, Summary, Action]
    extends Worker[Message, Work, Summary, Action] {
  implicit val as: ActorSystem
  override implicit val ec = as.dispatcher

  type MessageSource = Source[Message, NotUsed]
  type MessageSink = Sink[Action, Future[Done]]

  protected val parallelism: Int

  protected val source: MessageSource
  protected val sink: MessageSink

  def start: Future[Done] =
    source
      .mapAsyncUnordered(parallelism)(process)
      .toMat(sink)(Keep.right)
      .run()
}
