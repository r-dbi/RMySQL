context("Memory management")

test_that("opening query cancels existing query", {
  con <- RMySQL::mysqlDefault()
  rs1 <- dbSendQuery(con, "SHOW TABLES")
  expect_warning(rs2 <- dbSendQuery(con, "SHOW DATABASES"), "Cancelling previous query")

  expect_false(dbIsValid(rs1))

  dbClearResult(rs1)
  dbDisconnect(con)
})
