context("dbQuoteString")

test_that("quoting works", {
  if (!mysqlHasDefault()) skip("Test database not available")

  con <- dbConnect(MySQL(), dbname = "test")
  on.exit(dbDisconnect(con))

  expect_equal(dbQuoteString(con, "\\'"), SQL("'\\\\\\''"))
  expect_equal(dbQuoteString(con, "'"), SQL("'\\''"))
})
