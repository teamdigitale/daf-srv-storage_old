package it.gov.daf

import models._
import org.apache.spark.sql.DataFrame

package object dataset {
  import com.twitter.util.{Future => TwitterFuture, Promise => TwitterPromise, Return, Throw}
  import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise, ExecutionContext}
  import scala.util.{Success, Failure}

  /** Convert from a Twitter Future to a Scala Future */
  implicit class RichTwitterFuture[A](val tf: TwitterFuture[A]) extends AnyVal {
    def asScala(implicit e: ExecutionContext): ScalaFuture[A] = {
      val promise: ScalaPromise[A] = ScalaPromise()
      tf.respond {
        case Return(value) => promise.success(value)
        case Throw(exception) => promise.failure(exception)
      }
      promise.future
    }
  }

  /** Convert from a Scala Future to a Twitter Future */
  implicit class RichScalaFuture[A](val sf: ScalaFuture[A]) extends AnyVal {
    def asTwitter(implicit e: ExecutionContext): TwitterFuture[A] = {
      val promise: TwitterPromise[A] = new TwitterPromise[A]()
      sf.onComplete {
        case Success(value) => promise.setValue(value)
        case Failure(exception) => promise.setException(exception)
      }
      promise
    }
  }

  implicit class DafDataFrameWrapper(df: DataFrame) {

    val supportedFunctions = Set("count", "max", "mean", "min", "sum")

    /**
     *
     * @param columns
     * @return a dataframe with projection for the selected columns
     */
    def select(columns: Option[List[String]]): DataFrame = {
      columns match {
        case Some(list) if list.isEmpty => df

        case Some(list) if list.contains("*") => df

        case Some(list) =>
          val selection = list.toSet.intersect(df.columns.toSet).toArray
          if (selection.isEmpty) df
          else df.select(selection.head, selection.tail: _*)

        case None => df
      }
    }

    /**
      *
      * @param optGroupBy
      * @return a dataframe goruped only for valid columns and aggregation functions
      */
    def groupBy(optGroupBy: Option[GroupBy]): DataFrame = {
      optGroupBy match {
        case Some(groupBy) =>
          val columns = df.columns.toSet
          val conditions = groupBy
            .conditions
            .filter(c => supportedFunctions.contains(c.aggregationFunction))
            .filter(c => columns.contains(c.column))
            .map(c => c.column -> c.aggregationFunction)
            .toMap

          if (conditions.isEmpty) df
          else df.groupBy(groupBy.groupColumn).agg(conditions)
        case None => df
      }
    }

    /**
      *
      * @param conditions
      * @return
      */
    def where(conditions: Option[List[String]]): DataFrame =
      conditions match {
        case Some(cs) => cs.foldLeft(df)((d, c) =>  d.where(c))
        case None => df
      }
  }
}
