package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

trait VersionManagerFixtures {
  def withVersionManager[R](testWith: TestWith[VersionManager, R]): R = {
    val versionManager = new VersionManager()

    testWith(versionManager)
  }
}

class VersionManagerTest extends FunSpec with Matchers with ScalaFutures with ExternalIdentifierGenerators with VersionManagerFixtures {
  it("assigns v1 for an external ID/ingest ID it's never seen before") {
    withVersionManager { versionManager =>
      val future = versionManager.assignVersion(
        ingestId = createIngestID,
        ingestDate = Instant.now(),
        externalIdentifier = createExternalIdentifier
      )

      whenReady(future) { version =>
        version shouldBe 1
      }
    }
  }
}
