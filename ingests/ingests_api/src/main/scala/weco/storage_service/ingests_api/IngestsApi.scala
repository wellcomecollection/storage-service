package weco.storage_service.ingests_api

import java.util.UUID

import akka.http.scaladsl.server._
import weco.storage_service.display.ingests.RequestDisplayIngest
import weco.storage_service.ingests_api.responses.{CreateIngest, LookupIngest}

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
