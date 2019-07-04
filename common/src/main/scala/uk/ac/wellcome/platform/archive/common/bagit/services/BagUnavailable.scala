package uk.ac.wellcome.platform.archive.common.bagit.services

case class BagUnavailable(msg: String) extends Throwable(msg)
