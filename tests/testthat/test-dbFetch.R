context("dbFetch")

test_that("fetch with no arguments gets all rows", {
  con <- mysqlDefault()

  df <- data.frame(x = 1:1000)
  dbWriteTable(con, "test", df, temporary = TRUE)

  rs <- dbSendQuery(con, "SELECT * FROM test")
  expect_equal(nrow(dbFetch(rs)), 1000)

  dbDisconnect(con)
})

test_that("fetch progressively pulls in rows", {
  con <- mysqlDefault()

  df <- data.frame(x = 1:25)
  dbWriteTable(con, "test", df, temporary = TRUE)

  rs <- dbSendQuery(con, "SELECT * FROM test")
  expect_equal(nrow(dbFetch(rs, 10)), 10)
  expect_equal(nrow(dbFetch(rs, 10)), 10)
  expect_equal(nrow(dbFetch(rs, 10)), 5)

  dbDisconnect(con)
})
