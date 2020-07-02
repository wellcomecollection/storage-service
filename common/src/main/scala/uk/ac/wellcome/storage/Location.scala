package uk.ac.wellcome.storage

import java.nio.file.Paths

private object PathJoiner {
  def join(first: String, more: String*): String =
    Paths.get(first, more: _*).normalize().toString
}

trait Location {
  def toObjectLocation: ObjectLocation

  val namespace: String
  val path: String
}

trait Prefix[OfLocation <: Location] {
  val namespace: String
  val pathPrefix: String

  def asLocation(parts: String*): OfLocation

  def join(parts: String*): Prefix[OfLocation]

  def toObjectLocationPrefix: ObjectLocationPrefix

  def getRelativePathFrom(location: OfLocation): String
}

case class S3ObjectLocation(
  bucket: String,
  key: String
) extends Location {
  val namespace: String = bucket
  val path: String = key

  override def toString: String =
    s"s3://$bucket/$key"

  def toObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = this.bucket,
      path = this.key
    )

  def asPrefix: S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = this.bucket,
      keyPrefix = this.key
    )
}

case object S3ObjectLocation {
  def apply(location: ObjectLocation): S3ObjectLocation =
    S3ObjectLocation(
      bucket = location.namespace,
      key = location.path
    )
}

case class S3ObjectLocationPrefix(
  bucket: String,
  keyPrefix: String
) extends Prefix[S3ObjectLocation] {
  val namespace: String = bucket
  val pathPrefix: String = keyPrefix

  override def toString: String =
    s"s3://$bucket/$keyPrefix"

  override def asLocation(parts: String*): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bucket,
      key = PathJoiner.join(keyPrefix, parts: _*)
    )

  def toObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = this.bucket,
      path = this.keyPrefix
    )

  override def getRelativePathFrom(location: S3ObjectLocation): String = {
    assert(location.bucket == this.bucket)
    location.key.stripPrefix(this.keyPrefix)
  }

  override def join(parts: String*): Prefix[S3ObjectLocation] =
    S3ObjectLocationPrefix(
      bucket = bucket,
      keyPrefix = PathJoiner.join(keyPrefix, parts: _*)
    )
}

case object S3ObjectLocationPrefix {
  def apply(location: ObjectLocationPrefix): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = location.namespace,
      keyPrefix = location.path
    )
}

case class MemoryLocation(
  namespace: String,
  path: String
) extends Location {
  override def toString: String =
    s"mem://$namespace/$path"

  override def toObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = path
    )
}

case object MemoryLocation {
  def apply(location: ObjectLocation): MemoryLocation =
    MemoryLocation(
      namespace = location.namespace,
      path = location.path
    )
}

case class MemoryLocationPrefix(
  namespace: String,
  pathPrefix: String
) extends Prefix[MemoryLocation] {
  val path: String = pathPrefix

  override def toString: String =
    s"mem://$namespace/$pathPrefix"

  override def asLocation(parts: String*): MemoryLocation =
    MemoryLocation(
      namespace = namespace,
      path = PathJoiner.join(pathPrefix, parts: _*)
    )

  override def toObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = namespace,
      path = pathPrefix
    )

  override def getRelativePathFrom(location: MemoryLocation): String = {
    assert(location.namespace == this.namespace)
    location.path.stripPrefix(this.pathPrefix)
  }

  override def join(parts: String*): Prefix[MemoryLocation] =
    MemoryLocationPrefix(
      namespace = namespace,
      pathPrefix = PathJoiner.join(pathPrefix, parts: _*)
    )
}

case class AzureBlobItemLocation(
  container: String,
  name: String
) extends Location {
  val namespace: String = container
  val path: String = name

  override def toObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = container,
      path = name
    )
}

case class AzureBlobItemLocationPrefix(
  container: String,
  namePrefix: String
) extends Prefix[AzureBlobItemLocation] {
  val namespace: String = container
  val pathPrefix: String = namePrefix

  override def asLocation(parts: String*): AzureBlobItemLocation =
    AzureBlobItemLocation(
      container = container,
      name = Paths.get(namePrefix, parts: _*).normalize().toString
    )

  override def toObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = container,
      path = namePrefix
    )

  override def getRelativePathFrom(location: AzureBlobItemLocation): String = {
    assert(location.container == this.container)
    location.name.stripPrefix(this.namePrefix)
  }

  override def join(parts: String*): Prefix[AzureBlobItemLocation] =
    AzureBlobItemLocationPrefix(
      container = container,
      namePrefix = PathJoiner.join(namePrefix, parts: _*)
    )
}
