package it.gov.daf.dataset

import javax.inject._

import com.google.inject.ImplementedBy
import com.twitter.util.SimplePool
import org.apache.spark.sql.SparkSession
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[SparkEndpointImpl])
trait SparkEndpoint {
  def reserveSparkSession(implicit ec: ExecutionContext): Future[SparkSession]
}

/**
 * This class provides an abstraction over SparkSessions.
 * Right now it is backed via a singleton pool of local spark session but can be extended to use other approaches,
 * namely:
 * - livy
 * - mist
 * - spark job observer
 * @param lifecycle
 * @param configuration
 */
@Singleton
class SparkEndpointImpl @Inject() (lifecycle: ApplicationLifecycle, configuration: Configuration) extends SparkEndpoint {

  private val sparkSession = SparkSession.builder
    .master("local")
    .appName(configuration.get[String]("application.name"))
    .config("spark.driver.memory", configuration.get[String]("spark.driver.memory"))
    .getOrCreate()

  private val pool = new SimplePool(mutable.Queue(sparkSession))

  /**
   * return a future over the spark session
   * @param ec
   * @return
   */
  def reserveSparkSession(implicit ec: ExecutionContext): Future[SparkSession] = {
    pool.reserve().asScala
  }

  lifecycle.addStopHook { () =>
    Future.successful(sparkSession.close())
  }

}

