package uk.ac.wellcome.platform.storage.ingests_tracker.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import uk.ac.wellcome.platform.storage.ingests_tracker.IngestsTrackerApi
import uk.ac.wellcome.platform.storage.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestStoreUnexpectedError
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.storage.{StoreReadError, StoreWriteError, UpdateWriteError, Version}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

trait IngestsTrackerApiFixture
    extends IngestTrackerFixtures
    with IngestGenerators
    with Akka {

  val trackerUri = "http://localhost:8080"

  private def withApp[R](
    ingestTrackerTest: MemoryIngestTracker,
    callbackNotificationMessageSender: MemoryMessageSender =
      new MemoryMessageSender(),
    updatedIngestsMessageSender: MemoryMessageSender = new MemoryMessageSender()
  )(testWith: TestWith[IngestsTrackerApi[String, String], R]): R = {
    withActorSystem { implicit actorSystem =>
      val callbackNotificationService =
        new CallbackNotificationService(callbackNotificationMessageSender)

      val messagingService: MessagingService[String, String] =
        new MessagingService(
          callbackNotificationService,
          updatedIngestsMessageSender
        )

      val app = new IngestsTrackerApi[String, String](
        ingestTrackerTest,
        messagingService
      )()

      app.run()

      testWith(app)
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
        Left(IngestStoreUnexpectedError(StoreReadError(new Throwable("BOOM!"))))

      override def init(ingest: Ingest): Result =
        Left(IngestStoreUnexpectedError(StoreWriteError(new Throwable("BOOM!"))))

      override def update(update: IngestUpdate): Result =
        Left(IngestStoreUnexpectedError(UpdateWriteError(StoreWriteError(new Throwable("BOOM!")))))
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
