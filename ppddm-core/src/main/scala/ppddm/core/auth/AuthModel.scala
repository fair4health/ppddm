package ppddm.core.auth

final case class LoginRequest(username: String, password: String) {
  def toJson: String = {
    s"""{"password":"$password","username":"$username"}"""
  }
}
final case class LoginResponse(token_type: String, access_token: String, refresh_token: String, expires_in: Long, scope: String,
                         active: Boolean, jti: Option[String])

final case class AuthServerException(timestamp: String, status: Int, error: String, message: String, path: String)
