package weco.storage_service.ingests_tracker.fixtures

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, RequestEntity}
import org.scalatest.concurrent.ScalaFutures.whenReady
import org.scalatest.concurrent.{IntegrationPatience, PatienceConfiguration}
import org.scalatest.time.{Seconds, Span}
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.memory.MemoryMessageSender
import weco.storage.maxima.memory.MemoryMaxima
import weco.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import weco.storage.{StoreReadError, StoreWriteError, UpdateWriteError, Version}
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.{Ingest, IngestID, IngestUpdate}
import weco.storage_service.ingests_tracker.IngestsTrackerApi
import weco.storage_service.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}
import weco.storage_service.ingests_tracker.tracker.IngestStoreUnexpectedError
import weco.storage_service.ingests_tracker.tracker.memory.MemoryIngestTracker

trait IngestsTrackerApiFixture
    extends IngestTrackerFixtures
    with IngestGenerators
    with IntegrationPatience
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
        Left(
          IngestStoreUnexpectedError(StoreWriteError(new Throwable("BOOM!")))
        )

      override def update(update: IngestUpdate): Result =
        Left(
          IngestStoreUnexpectedError(
            UpdateWriteError(StoreWriteError(new Throwable("BOOM!")))
          )
        )
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

  private def whenRequestReady[R](
    r: HttpRequest
  )(testWith: TestWith[HttpResponse, R]): R =
    withActorSystem { implicit actorSystem =>
      val request = Http().singleRequest(r)
      whenReady(
        request,
        timeout = PatienceConfiguration.Timeout(Span.apply(5, Seconds))
      ) { response: HttpResponse =>
        testWith(response)
      }
    }

  def whenAbsoluteGetRequestReady[R](
    path: String
  )(testWith: TestWith[HttpResponse, R]): R = {
    val request = HttpRequest(
      method = GET,
      uri = s"$path"
    )

    whenRequestReady(request) { response =>
      testWith(response)
    }
  }

  def whenAbsolutePostRequestReady[R](
    path: String,
    entity: RequestEntity
  )(
    testWith: TestWith[HttpResponse, R]
  ): R = {
    val request = HttpRequest(
      method = POST,
      uri = s"$path",
      entity = entity
    )

    whenRequestReady(request) { response =>
      testWith(response)
    }
  }
}
