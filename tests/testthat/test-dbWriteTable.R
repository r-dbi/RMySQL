context("dbWriteTable")

test_that("options work", {
  if (!mysqlHasDefault()) skip("Test database not available")

  con <- dbConnect(MySQL(), dbname = "test")
  on.exit(dbDisconnect(con))

  expected <- data.frame(
    a = c(1:3, NA),
    b = c("x", "y", "z", "E"),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "dat", "dat-n.txt", sep="|", eol="\n", overwrite = TRUE)
  expect_equal(dbReadTable(con, "dat"), expected)
})
