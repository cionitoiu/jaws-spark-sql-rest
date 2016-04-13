package server.api

import implementation.HiveContextWrapper
import org.apache.spark.SparkContext
import server.Configuration
import spray.http._
import spray.routing._
import authentikat.jwt._
import com.nimbusds.jose._
import org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString
import org.apache.commons.codec.binary.Base64.decodeBase64

/**
  * Created by cristianionitoiu on 31/03/16.
  */

trait SecurityApi extends HttpService {

  // The hive context that is initialized when a new user invokes JAWS
  //val hiveEnvironment: Map[String, HiveContextWrapper] = Map()
  //var hiveContext: HiveContextWrapper = _
  // The spark context that is initialized when the server starts
  //var sparkContext: SparkContext = _


  def securityFilter: Directive1[String] = {
    optionalHeaderValueByName("Authorization").flatMap {
      case Some(value) =>
        try {
          val isValid = SecurityApi.validate(value, "my secret")
          if (isValid) {
            val jwtParts = JWSObject.parse(value)
            Configuration.log4j.info("Jwt Header " + jwtParts.getHeader.toJSONObject.toString)
            Configuration.log4j.info("Jwt Payload " + jwtParts.getPayload.toJSONObject.toString)

            //extract user id
            Option(jwtParts.getPayload.toJSONObject.get("sub").toString) match {
              case Some(value) => provide(value)
              case None => complete("Incomplete authorization!") //Client.Error
            }
          } else {
            complete("Invalid authorization!") //ClientError
          }
        } catch {
          case e: Exception => complete(StatusCodes.InternalServerError, e.getMessage)
        }
      case None => complete(StatusCodes.BadRequest, "Missing authorization!")
    }
  }
}

object SecurityApi {

  /**
    * From: jasongoodwin/authentikat-jwt
    * Validate a JWT claims set against a secret key.
    * Validates an un-parsed jwt as parsing it before validating it is probably un-necessary.
    * Note this does NOT validate exp or other validation claims - it only validates the claims against the hash.
    *
    * @param jwt
    * @param key
    * @return
    */

  def validate(jwt: String, key: String): Boolean = {

    import org.json4s.DefaultFormats
    implicit val formats = DefaultFormats

    jwt.split("\\.") match {
      case Array(providedHeader, providedClaims, providedSignature) =>

        val headerJsonString = new String(decodeBase64(providedHeader), "UTF-8")
        val header = JwtHeader.fromJsonStringOpt(headerJsonString).getOrElse(JwtHeader(None, None, None))

        val signature = encodeBase64URLSafeString(
          JsonWebSignature(header.algorithm.getOrElse("none"), providedHeader + "." + providedClaims, key))

        providedSignature.contentEquals(signature)
      case _ =>
        false
    }
  }

}
