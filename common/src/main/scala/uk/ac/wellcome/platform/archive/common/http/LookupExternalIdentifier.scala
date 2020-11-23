package uk.ac.wellcome.platform.archive.common.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

trait LookupExternalIdentifier {

  // Decode an external identifier provided as part of a URL.
  //
  // Sometimes we have an external identifier with slashes or spaces.
  // For maximum flexibility, we want to support both URL-encoded
  // and complete path versions.
  //
  // For example, if the external identifier is "alfa/bravo",
  // you could look up both of:
  //
  //    /bags/space-id/alfa/bravo
  //    /bags/space-id/alfa%2Fbravo
  //
  protected def decodeExternalIdentifier(s: String): ExternalIdentifier =
    ExternalIdentifier(
      URLDecoder.decode(s, StandardCharsets.UTF_8.toString)
    )
}
