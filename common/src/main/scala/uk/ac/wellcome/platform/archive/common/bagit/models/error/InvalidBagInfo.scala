package uk.ac.wellcome.platform.archive.common.bagit.models.error

case class InvalidBagInfo[T](t: T, keys: List[String]) {
  override def toString = s"Invalid bag-info: ${keys.mkString(", ")}"
}
