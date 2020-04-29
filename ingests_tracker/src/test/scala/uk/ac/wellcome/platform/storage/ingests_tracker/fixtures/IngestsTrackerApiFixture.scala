package uk.ac.wellcome.platform.storage.ingests_tracker.fixtures

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestStoreUnexpectedError,
  IngestTracker
}
import uk.ac.wellcome.platform.storage.ingests_tracker.IngestsTrackerApi
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

trait IngestsTrackerApiFixture
    extends IngestTrackerFixtures
    with IngestGenerators
    with MetricsSenderFixture {

  private def withApp[R](
    ingestTrackerTest: MemoryIngestTracker
  )(testWith: TestWith[IngestsTrackerApi, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val app = new IngestsTrackerApi() {
          override val ingestTracker: IngestTracker = ingestTrackerTest
          override implicit lazy protected val sys: ActorSystem = actorSystem
          override implicit lazy protected val mat: ActorMaterializer = materializer
        }

        app.run()

        testWith(app)
      }
    }

  def withBrokenApp[R](testWith: TestWith[MemoryIngestTracker, R]): R = {

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
    }

    withApp(brokenTracker) { _ =>
      testWith(brokenTracker)
    }
  }

  def withConfiguredApp[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[MemoryIngestTracker, R]
  ): R = withMemoryIngestTracker(initialIngests) { ingestTracker =>
    withApp(ingestTracker) { _ =>
      testWith(ingestTracker)
    }
  }
}
