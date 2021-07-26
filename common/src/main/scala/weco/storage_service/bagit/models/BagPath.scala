package weco.storage_service.bagit.models

import weco.json.{TypedString, TypedStringOps}

class BagPath(val value: String) extends TypedString[BagPath] {
  val underlying: String = value
}

object BagPath extends TypedStringOps[BagPath] {
  override def apply(underlying: String): BagPath = new BagPath(underlying)
}
