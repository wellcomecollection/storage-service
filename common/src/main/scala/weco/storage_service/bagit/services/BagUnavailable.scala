package weco.storage_service.bagit.services

case class BagUnavailable(msg: String) extends Throwable(msg)
