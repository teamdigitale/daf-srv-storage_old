package it.gov.daf

import play.api.mvc._

trait HdfsUtils {

  def impersonate(action: Request[AnyContent] => Result) = (req: Request[AnyContent]) => {
  }
}
