context("tables")

types <- list(
  logical = c(TRUE, FALSE, NA),
  integer = c(1:5, NA),
  double = c(runif(5), NA),
  character = c("a", "b", NA)
)

round_trip <- function(x) {
  con <- mysqlDefault()
  dbWriteTable(con, "round_trip_test", data.frame(x = x), temporary = TRUE)
  y <- dbReadTable(con, "round_trip_test")[[1]]
  dbDisconnect(con)

  y
}

test_that("can round trip atomic vectors", {
  expect_equal(round_trip(types$logical), as.integer(types$logical))
  expect_equal(round_trip(types$integer), types$integer)
  expect_equal(round_trip(types$double), types$double)
  expect_equal(round_trip(types$character), types$character)
})
