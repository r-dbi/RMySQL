context("queries")

test_that("query returns expected number of rows", {
  if (!mysqlHasDefault()) skip("Test database not available")

  conn <- dbConnect(RMySQL::MySQL(), dbname = "test")
  dbRemoveTable(conn, "iris")
  dbWriteTable(conn, 'iris', datasets::iris, row.names = FALSE)
  rs <- dbSendQuery(conn, "SELECT * FROM iris WHERE Species='versicolor'")

  x <- dbFetch(rs, n = 2)
  expect_equal(nrow(x), 2)
  expect_equal(dbGetRowCount(rs), 2)

  dbClearResult(rs)
  dbRemoveTable(conn, "iris")
  dbDisconnect(conn)
})

test_that("correctly computes affected rows", {
  if (!mysqlHasDefault()) skip("Test database not available")

  conn <- dbConnect(RMySQL::MySQL(), dbname="test")
  dbRemoveTable(conn, "iris")
  dbWriteTable(conn, 'iris', datasets::iris, row.names = FALSE)
  rs <- dbSendQuery(conn, "DELETE FROM iris WHERE Species = 'versicolor'")

  expect_equal(dbGetRowsAffected(rs), sum(iris$Species == 'versicolor'))

  dbClearResult(rs)
  dbRemoveTable(conn, "iris")
  dbDisconnect(conn)
})
