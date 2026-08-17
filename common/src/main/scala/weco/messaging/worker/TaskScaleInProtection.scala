package weco.messaging.worker

import grizzled.slf4j.Logging

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

/** Marks the running ECS task as protected from autoscaling scale-in while it
  * has messages in flight, so a worker can't be killed mid-work.
  *
  * See
  * https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html
  */
trait TaskScaleInProtection {
  def acquire(): Unit
  def release(): Unit
}

object NoOpTaskScaleInProtection extends TaskScaleInProtection {
  override def acquire(): Unit = ()
  override def release(): Unit = ()
}

class EcsTaskScaleInProtection(agentUri: String, expiresInMinutes: Int)
    extends TaskScaleInProtection
    with Logging {

  private var inFlight = 0

  // Only call the agent on 0<->1 transitions, to stay well inside the
  // UpdateTaskProtection rate limits on busy queues.
  override def acquire(): Unit = synchronized {
    inFlight += 1
    if (inFlight == 1) {
      setProtection(enabled = true)
    }
  }

  override def release(): Unit = synchronized {
    inFlight -= 1
    if (inFlight == 0) {
      setProtection(enabled = false)
    }
  }

  private lazy val client =
    HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(2))
      .build()

  // Failures are logged and swallowed: losing protection is survivable,
  // failing the message is not.
  protected def setProtection(enabled: Boolean): Unit =
    try {
      val body =
        if (enabled)
          s"""{"ProtectionEnabled":true,"ExpiresInMinutes":$expiresInMinutes}"""
        else
          """{"ProtectionEnabled":false}"""

      val request =
        HttpRequest
          .newBuilder()
          .uri(URI.create(s"$agentUri/task-protection/v1/state"))
          .timeout(Duration.ofSeconds(5))
          .header("Content-Type", "application/json")
          .PUT(HttpRequest.BodyPublishers.ofString(body))
          .build()

      val response = client.send(request, HttpResponse.BodyHandlers.ofString())

      if (response.statusCode() != 200) {
        warn(
          s"Failed to set task scale-in protection (enabled=$enabled): HTTP ${response
              .statusCode()} ${response.body()}"
        )
      }
    } catch {
      case e: Exception =>
        warn(s"Failed to set task scale-in protection (enabled=$enabled)", e)
    }
}

object TaskScaleInProtection {
  // Backstop expiry if a task dies without releasing; must outlast our
  // slowest work, which is bag replication (5 hour visibility timeout).
  private val expiresInMinutes = 330

  /** Protection against the real ECS agent if we're running in ECS, otherwise
    * (local, CI) a no-op.
    */
  lazy val default: TaskScaleInProtection =
    sys.env.get("ECS_AGENT_URI") match {
      case Some(uri) => new EcsTaskScaleInProtection(uri, expiresInMinutes)
      case None      => NoOpTaskScaleInProtection
    }
}
