package uk.ac.wellcome.platform.storage.ingests.api

import java.util.UUID

import akka.http.scaladsl.server._
import weco.storage_service.display.ingests.RequestDisplayIngest
import uk.ac.wellcome.platform.storage.ingests.api.responses.{
  CreateIngest,
  LookupIngest
}

trait IngestsApi[UnpackerDestination]
    extends CreateIngest[UnpackerDestination]
    with LookupIngest {

  val ingests: Route = pathPrefix("ingests") {
    post {
      entity(as[RequestDisplayIngest]) { ingest =>
        withFuture { createIngest(ingest) }
      }
    } ~ path(JavaUUID) { id: UUID =>
      get {
        withFuture { lookupIngest(id) }
      }
    }
  }
}
