package au.org.ala.biocache.load

import java.util.Date
import java.time.OffsetDateTime

/**
 * Object representing a data resource configuration.
 */
case class DataResourceConfig(protocol:String, urls:Seq[String], uniqueTerms:Seq[String],
                         connectionParams:Map[String,String],
                         customParams:Map[String,String],
                         dateLastChecked:Option[OffsetDateTime] = None){}
