package uk.ac.wellcome.platform.archive.common.bagit.models.error

case class InvalidBagInfo(keys: List[String]) {
  override def toString = s"Invalid bag-info: ${keys.mkString(", ")}"
}
