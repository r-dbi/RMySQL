### Compatibility fix for TSMySQL
#' @export
#' @rdname db-meta
setClass("dbObjectId")
setAs("dbObjectId", "integer",
  def = function(from) as(slot(from,"Id"), "integer")
)
