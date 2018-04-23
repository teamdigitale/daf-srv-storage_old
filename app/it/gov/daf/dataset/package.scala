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

  implicit class DafDataFrameWrapper(dataframe: DataFrame) {
    val supportedFunctions = Set("count", "max", "mean", "min", "sum")

    /**
     *
     * @param columns
     * @return a dataframe with projection for the selected columns
     */
    def select(columns: Option[List[String]]): DataFrame = columns match {
      case None => dataframe
      // handle case when no columns all * is selected
      case Some(list) if list.isEmpty => dataframe
      case Some(list) if list.contains("*") => dataframe
      case Some(list) => dataframe.select(list.head, list.tail: _*)
    }

    /**
     *
     * @param optGroupBy
     * @return a dataframe goruped only for valid columns and aggregation functions
     */
    def groupBy(optGroupBy: Option[GroupBy]): DataFrame = {
      optGroupBy match {
        case Some(groupBy) =>

          val columns = dataframe.columns.toSet
          val conditions = groupBy
            .conditions
            .filter(c => supportedFunctions.contains(c.aggregationFunction))
            .filter(c => columns.contains(c.column))
            .map(c => c.column -> c.aggregationFunction)
            .toMap

          if (conditions.isEmpty) dataframe
          else dataframe.groupBy(groupBy.groupColumn).agg(conditions)

        case None => dataframe
      }
    }

    def where(conditions: Option[List[WhereCondition]]): DataFrame = {
      conditions.getOrElse(List.empty)
          .foldLeft(dataframe)((df, cond) => df.where(cond.toSql()))
    }
  }
}
