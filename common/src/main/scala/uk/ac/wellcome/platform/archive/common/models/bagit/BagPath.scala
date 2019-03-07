package uk.ac.wellcome.platform.archive.common.models.bagit

case class BagPath(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object BagPath {
  def create(identifier: ExternalIdentifier): BagPath =
    BagPath(identifier.underlying)
}
