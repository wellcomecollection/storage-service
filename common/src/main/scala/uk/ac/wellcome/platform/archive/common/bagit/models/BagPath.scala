package uk.ac.wellcome.platform.archive.common.bagit.models

case class BagPath(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
