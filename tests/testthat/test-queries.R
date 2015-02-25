context("queries")

test_that("query returns expected number of rows", {
  conn <- mysqlDefault()
  dbWriteTable(conn, 'iris', datasets::iris, temporary = TRUE)
  rs <- dbSendQuery(conn, "SELECT * FROM iris WHERE Species='versicolor'")

  x <- dbFetch(rs, n = 2)
  expect_equal(nrow(x), 2)
  expect_equal(dbGetRowCount(rs), 2)

  x <- dbFetch(rs, n = 2)
  expect_equal(nrow(x), 2)
  expect_equal(dbGetRowCount(rs), 4)

  dbClearResult(rs)
  dbDisconnect(conn)
})

test_that("correctly computes affected rows", {
  conn <- mysqlDefault()
  dbWriteTable(conn, 'iris', datasets::iris, temporary = TRUE)
  rs <- dbSendQuery(conn, "DELETE FROM iris WHERE Species = 'versicolor'")

  expect_equal(dbGetRowsAffected(rs), sum(iris$Species == 'versicolor'))

  dbClearResult(rs)
  dbDisconnect(conn)
})

test_that("modification query is always complete", {
  conn <- mysqlDefault()
  dbWriteTable(conn, 'iris', datasets::iris, temporary = TRUE)
  rs <- dbSendQuery(conn, "DELETE FROM iris WHERE Species = 'versicolor'")

  expect_true(dbHasCompleted(rs))

  dbClearResult(rs)
  dbDisconnect(conn)
})


test_that("setting parameter query is always complete", {
  conn <- mysqlDefault()
  rs <- dbSendQuery(conn, 'SET time_zone = "+00:00"')

  expect_true(dbHasCompleted(rs))

  dbClearResult(rs)
  dbDisconnect(conn)
})
