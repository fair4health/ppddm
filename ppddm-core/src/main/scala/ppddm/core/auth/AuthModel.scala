package ppddm.core.auth

import ppddm.core.util.JsonClass

/**
 * The model classes for authentication communication with the Auth server (Keycloak of ATOS)
 */

final case class LoginRequest(username: String, password: String) extends JsonClass
final case class LoginResponse(token_type: String, access_token: String, refresh_token: String, expires_in: Long, scope: String,
                         active: Boolean, jti: Option[String]) extends JsonClass

final case class AuthServerException(timestamp: String, status: Int, error: String, message: String, path: String)
