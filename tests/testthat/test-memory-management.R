context("Memory management")

test_that("querying closed connection throws error", {
  db <- mysqlDefault()
  dbDisconnect(db)

  expect_error(dbGetQuery(db, "select * from foo"), "not valid")
})

test_that("accessing cleared result throws error", {
  con <- mysqlDefault()
  rs <- dbSendQuery(con, "SELECT 1;")
  dbClearResult(rs)

  expect_error(dbFetch(rs), "not valid")

  dbDisconnect(con)
})

test_that("opening query cancels existing query", {
  con <- RMySQL::mysqlDefault()
  rs1 <- dbSendQuery(con, "SHOW TABLES")
  expect_warning(rs2 <- dbSendQuery(con, "SHOW DATABASES"), "Cancelling previous query")

  expect_false(dbIsValid(rs1))

  dbClearResult(rs1)
  dbDisconnect(con)
})
