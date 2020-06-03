package uk.ac.wellcome.platform.archive.common.bagit.models

import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

case class BagVersion(underlying: Int) extends AnyVal {
  override def toString: String = s"v$underlying"

  def increment: BagVersion =
    BagVersion(underlying + 1)
}

object BagVersion {
  // Matches a version string.  The versions should be of the form 'v1', 'v2', ...
  private val versionRegex: Regex = new Regex("^v(\\d+)$", "version")

  def fromString(versionString: String): Try[BagVersion] =
    versionRegex.findFirstMatchIn(versionString) match {
      case Some(regexMatch) =>
        Success(
          BagVersion(regexMatch.group("version").toInt)
        )
      case None =>
        Failure(
          new Throwable(s"Could not parse version string: $versionString")
        )
    }

  implicit val encoder: Encoder[BagVersion] =
    (version: BagVersion) => Json.fromInt(version.underlying)

  implicit val decoder: Decoder[BagVersion] = (cursor: HCursor) =>
    cursor.value.as[Int].map { BagVersion(_) }

  implicit def evidence: DynamoFormat[BagVersion] =
    DynamoFormat.iso[BagVersion, Int](
      BagVersion(_)
    )(
      _.underlying
    )

  implicit val ordering: Ordering[BagVersion] =
    (x: BagVersion, y: BagVersion) =>
      Ordering[Int].compare(x.underlying, y.underlying)
}
