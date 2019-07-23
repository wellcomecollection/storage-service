package uk.ac.wellcome.platform.archive.common.bagit.services

sealed trait BagReadError extends Throwable {
  val e: Throwable
}

case class BagInfoReadError(e: Throwable) extends Throwable(e) with BagReadError
case class BagManifestReadError(e: Throwable)
    extends Throwable(e)
    with BagReadError
case class TagManifestReadError(e: Throwable)
    extends Throwable(e)
    with BagReadError
case class BagFetchReadError(e: Throwable)
    extends Throwable(e)
    with BagReadError
