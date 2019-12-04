package uk.ac.wellcome.platform.storage.ingests.api

import java.util.UUID

import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.display.RequestDisplayIngest
import uk.ac.wellcome.platform.storage.ingests.api.responses.{
  CreateIngest,
  LookupIngest
}

trait IngestsApi extends CreateIngest with LookupIngest {
  val ingests: Route = pathPrefix("ingests") {
    post {
      entity(as[RequestDisplayIngest]) { createIngest }
    } ~ path(JavaUUID) { id: UUID =>
      get {
        lookupIngest(id)
      }
    }
  }
}
