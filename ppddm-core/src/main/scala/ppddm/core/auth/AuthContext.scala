package ppddm.core.auth

import org.json4s.JsonAST.{JInt, JLong, JObject}
import org.json4s.jackson.JsonMethods._

import java.nio.charset.StandardCharsets
import java.time.{LocalDateTime, ZoneId}
import java.util.Base64

/**
 * Holds the contextual information of the authentication such as the JWT payload and the expiration time.
 *
 * @param accessToken JWT of the user
 */
class AuthContext(accessToken: String) {

  private var payload: String = _
  private var expirationTime: Long = _
  parseAccessToken()

  private def parseAccessToken(): Unit = {
    val payloadEncoded = accessToken.split('.')(1)
    payload = new String(Base64.getDecoder.decode(payloadEncoded), StandardCharsets.UTF_8)
    expirationTime = (parse(payload) \\ "exp").asInstanceOf[JInt].num.longValue()
  }

  def isExpired: Boolean = {
    LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant.toEpochMilli / 1000 > this.expirationTime
  }
}
