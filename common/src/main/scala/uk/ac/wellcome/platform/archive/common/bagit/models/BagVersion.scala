package uk.ac.wellcome.platform.archive.common.bagit.models

case class BagVersion(underlying: Int) extends AnyVal {
  override def toString: String = s"v$underlying"

  def increment: BagVersion =
    BagVersion(underlying + 1)
}
