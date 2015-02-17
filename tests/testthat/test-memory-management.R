context("Memory management")

test_that("querying closed connection throws error", {
  db <- mysqlDefault()
  dbDisconnect(db)

  expect_error(dbGetQuery(db, "select * from foo"), "not valid")
})

test_that("accessing cleared result throws error", {
  res <- dbSendQuery(mysqlDefault(), "SELECT 1;")
  dbClearResult(res)

  expect_error(dbFetch(res), "not valid")
})
