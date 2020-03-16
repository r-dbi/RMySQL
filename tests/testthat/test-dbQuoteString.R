context("dbQuoteString")

test_that("quoting works", {
  if (!mysqlHasDefault()) skip("Test database not available")

  con <- dbConnect(MySQL(), dbname = "test")
  expect_equal(dbQuoteString(con, "\\'"), SQL("'\\\\\\''"))
  expect_equal(dbQuoteString(con, "'"), SQL("'\\''"))
  on.exit(dbDisconnect(con))
})
