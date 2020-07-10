package ppddm.core.fhir.r4.resources

import java.time.LocalDateTime

import ppddm.core.fhir.r4.datatypes.{Identifier, Meta, Signature}

object BundleType extends Enumeration {
  type BundleType = Value
  val document, message, transaction, `transaction-response`, batch,
    `batch-response`, history, searchset, collection = Value
}

object BundleEntrySearchMode extends Enumeration {
  type BundleEntrySearchMode = Value
  val `match`, include, outcome = Value
}

object BundleEntryRequestMethod extends Enumeration {
  type BundleEntryRequestMethod = Value
  val GET, HEAD, POST, PUT, DELETE, PATCH = Value
}

class BundleLink(val relation: String,
                 val url: String // type: uri
                )

class BundleEntrySearch(val mode: Option[String], // enum: BundleEntrySearchMode
                        val score: Option[Double])

class BundleEntryRequest(val method: String, // enum BundleEntryRequestMethod
                         val url: String, // type: uri
                         val ifNoneMatch: Option[String],
                         val ifModifiedSince: Option[LocalDateTime], // type: instant
                         val ifMatch: Option[String],
                         val ifNoneExist: Option[String])

class BundleEntryResponse[T <: Resource](val status: String,
                          val location: Option[String], // type: uri
                          val etag: Option[String],
                          val lastModified: Option[LocalDateTime], // type: instant
                          val outcome: Option[T])

class BundleEntry[T <: Resource](val link: Option[List[BundleLink]],
                  val fullUrl: Option[String], // type: uri
                  val resource: Option[T],
                  val search: Option[BundleEntrySearch],
                  val request: Option[BundleEntryRequest],
                  val response: Option[BundleEntryResponse[T]])

class Bundle[T <: Resource](override val id: Option[String],
             override val meta: Option[Meta],
             override val implicitRules: Option[String], // type: uri
             override val language: Option[String], // type: code
             val identifier: Option[List[Identifier]],
             val `type`: String, // enum: BundleType
             val timestamp: Option[LocalDateTime], // type: instant
             val total: Option[Int], // type: unsignedInt
             val link: Option[List[BundleLink]],
             val entry: Option[List[BundleEntry[T]]],
             val signature: Option[Signature]
            ) extends Resource("Bundle", id, meta, implicitRules, language) {

}
