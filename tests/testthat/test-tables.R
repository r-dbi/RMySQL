context("tables")

types <- list(
  logical = c(TRUE, FALSE, NA),
  integer = c(1:5, NA),
  double = c(runif(5), NA),
  character = c("a", "b", NA),
  date = Sys.Date() + c(1:2, NA),
  time = as.POSIXct(Sys.Date()) + c(1:2, NA)
)

test_that("can round trip atomic vectors", {
  expect_equal(roundTrip(types$logical), as.integer(types$logical))
  expect_equal(roundTrip(types$integer), types$integer)
  expect_equal(roundTrip(types$double), types$double)
  expect_equal(roundTrip(types$character), types$character)
  expect_equal(roundTrip(types$date), types$date)
  expect_equal(roundTrip(types$time), types$time)
})
