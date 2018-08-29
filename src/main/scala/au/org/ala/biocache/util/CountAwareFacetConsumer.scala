package au.org.ala.biocache.util

import au.org.ala.biocache.tool.DuplicationDetection
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.BlockingQueue
import org.slf4j.LoggerFactory

class CountAwareFacetConsumer(q: BlockingQueue[String], id: Int, proc: Array[String] => Unit, countSize: Int = 0, minSize: Int = 1) extends Thread {
  val logger = LoggerFactory.getLogger("CountAwareFacetConsumer")
  // FIXME: Should use a sentinel for the normal case rather than a boolean
  var shouldStop = false

  override def run() {
    val buf = new ArrayBuffer[String]()
    var counter = 0
    var batchSize = 0
    // FIXME: shouldStop doesn't look like it is ever accessed, how does this loop complete currently?
    while (!shouldStop || q.size() > 0) {
      try {
        // FIXME: This could cause data loss if lags occur, need to do this properly with a sentinel
        //wait 1 second before assuming that the queue is empty
        val value = q.poll(1, java.util.concurrent.TimeUnit.SECONDS)
        if (value != null) {
          if(logger.isDebugEnabled()) {
            logger.debug("Count Aware Consumer " + id + " is handling " + value)
          }
          val values = value.split("\t")
          val count = Integer.parseInt(values(1))
          if (count >= minSize) {
            counter += count
            batchSize += 1
            buf += values(0)
            if (counter >= countSize || batchSize == 200) {
              val array = buf.toArray
              buf.clear()
              counter = 0
              batchSize = 0
              proc(array)
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping " + id)
    }
  }
}