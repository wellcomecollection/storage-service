package uk.ac.wellcome.platform.storage.ingests_tracker.fixtures

import akka.actor.ActorSystem
import akka.stream.Materializer
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestStoreUnexpectedError,
  IngestTracker
}
import uk.ac.wellcome.platform.storage.ingests_tracker.IngestsTrackerApi
import uk.ac.wellcome.platform.storage.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

trait IngestsTrackerApiFixture
    extends IngestTrackerFixtures
    with IngestGenerators
    with Akka {

  private def withApp[R](
    ingestTrackerTest: MemoryIngestTracker,
    callbackNotificationMessageSender: MemoryMessageSender =
      new MemoryMessageSender(),
    updatedIngestsMessageSender: MemoryMessageSender = new MemoryMessageSender()
  )(testWith: TestWith[IngestsTrackerApi[String, String], R]): R = {
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val callbackNotificationService =
          new CallbackNotificationService(callbackNotificationMessageSender)

        val app = new IngestsTrackerApi[String, String] {

          override val messagingService: MessagingService[String, String] =
            new MessagingService(
              callbackNotificationService,
              updatedIngestsMessageSender
            )

          override val ingestTracker: IngestTracker = ingestTrackerTest
          override implicit lazy protected val sys: ActorSystem = actorSystem
          override implicit lazy protected val mat: Materializer =
            materializer
        }

        app.run()

        testWith(app)
      }
    }

  }

  def withBrokenIngestsTrackerApi[R](
    testWith: TestWith[
      (MemoryMessageSender, MemoryMessageSender, MemoryIngestTracker),
      R
    ]
  ): R = {
    val brokenTracker = new MemoryIngestTracker(
      underlying = new MemoryVersionedStore[IngestID, Ingest](
        new MemoryStore[Version[IngestID, Int], Ingest](
          initialEntries = Map.empty
        ) with MemoryMaxima[IngestID, Ingest]
      )
    ) {
      override def get(id: IngestID): Result =
        Left(IngestStoreUnexpectedError(new Throwable("BOOM!")))

      override def init(ingest: Ingest): Result =
        Left(IngestStoreUnexpectedError(new Throwable("BOOM!")))

      override def update(update: IngestUpdate): Result =
        Left(IngestStoreUnexpectedError(new Throwable("BOOM!")))
    }

    val callbackSender = new MemoryMessageSender()
    val ingestsSender = new MemoryMessageSender()

    withApp(brokenTracker) { _ =>
      val out = (callbackSender, ingestsSender, brokenTracker)

      testWith(out)
    }
  }

  def withIngestsTrackerApi[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[
      (MemoryMessageSender, MemoryMessageSender, MemoryIngestTracker),
      R
    ]
  ): R = withMemoryIngestTracker(initialIngests) { ingestTracker =>
    val callbackSender = new MemoryMessageSender()
    val ingestsSender = new MemoryMessageSender()

    withApp(ingestTracker, callbackSender, ingestsSender) { _ =>
      val out = (callbackSender, ingestsSender, ingestTracker)

      testWith(out)
    }
  }
}
