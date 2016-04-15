package server.api

import akka.actor.Props
import apiactors.ActorsPaths._
import apiactors.HiveUserImpersonationActor
import com.xpatterns.jaws.data.contracts.DAL
import server.Configuration
import server.MainActors._
import spray.http._
import spray.routing._
import authentikat.jwt._
import com.nimbusds.jose._
import org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString
import org.apache.commons.codec.binary.Base64.decodeBase64
import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by cristianionitoiu on 31/03/16.
  */

trait SecurityApi extends HttpService {
  // The hdfs configuration that is initialized when the server starts
  var hdfsConf: org.apache.hadoop.conf.Configuration = _

  // Holds the DAL. It is initialized when the server starts
  var dals: DAL = _

  //Holds Jaws user group information initialized at server start-up
  var realUgi : UserGroupInformation = _

  lazy val hiveActor = createActor(Props(new HiveUserImpersonationActor(realUgi, dals, hdfsConf)), HIVE_ACTOR_NAME, remoteSupervisor)

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
              case Some(userId) => provide(userId)
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
