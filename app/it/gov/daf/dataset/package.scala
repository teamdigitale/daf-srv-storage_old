package it.gov.daf

import models._
import org.apache.spark.sql.DataFrame

import scala.util.Try

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

  implicit class DafDataFrameWrapper(tDf: Try[DataFrame]) {

    val supportedFunctions = Set("count", "max", "mean", "min", "sum")

    /**
     *
     * @param columns
     * @return a dataframe with projection for the selected columns
     */
    def select(columns: Option[List[String]]): Try[DataFrame] = {
      columns match {
        case Some(list) if list.isEmpty => tDf

        case Some(list) if list.contains("*") => tDf

        case Some(list) =>
          tDf.flatMap{ df =>
            val selection = list.toSet.intersect(df.columns.toSet).toArray
            if (selection.isEmpty)
              Failure(
                new IllegalArgumentException(
                  s"columns ${list.mkString("["," , ","]")} not in ${df.columns.toSeq.mkString("["," , ","]")}"
                )
              )
            else Success(df.select(selection.head, selection.tail: _*))
          }

        case None => tDf
      }
    }

    /**
      *
      * @param optGroupBy
      * @return a dataframe goruped only for valid columns and aggregation functions
      */
    def groupBy(optGroupBy: Option[GroupBy]): Try[DataFrame] = {
      optGroupBy match {
        case Some(groupBy) =>
          tDf.map {df =>
            val columns = df.columns.toSet
            val conditions = groupBy
              .conditions
              .filter(c => supportedFunctions.contains(c.aggregationFunction))
              .filter(c => columns.contains(c.column))
              .map(c => c.column -> c.aggregationFunction)
              .toMap

            if (conditions.isEmpty) df
            else df.groupBy(groupBy.groupColumn).agg(conditions)
          }

        case None => tDf
      }
    }

    /**
      *
      * @param conditions
      * @return
      */
    def where(conditions: Option[List[String]]): Try[DataFrame] =
      conditions match {
        case Some(cs) =>
          tDf.map(df => cs.foldLeft(df)((d, c) =>  d.where(c)))

        case None => tDf
      }

    def limit(n: Int): Try[DataFrame] = {
      tDf.map(_.limit(n))
    }
  }
}
