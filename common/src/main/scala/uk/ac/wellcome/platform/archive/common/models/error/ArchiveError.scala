package uk.ac.wellcome.platform.archive.common.models.error

trait ArchiveError[T] {
  val t: T
}

case class InvalidBagInfo[T](t: T, keys: List[String]) extends ArchiveError[T] {
  override def toString = s"Invalid bag-info: ${keys.mkString(", ")}"
}
