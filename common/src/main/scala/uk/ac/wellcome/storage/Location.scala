package uk.ac.wellcome.storage

trait Location

case class S3ObjectLocation(
  bucket: String,
  key: String
) extends Location
