context("tables")

test_that("basic roundtrip is succesful", {
  if (!mysqlHasDefault()) skip("Test database not available")

  myDF <- data.frame(
    x = paste("x", 1:5, sep = ""),
    y = paste("y", 1:5, sep = ""),
    row.names = letters[1:5],
    stringsAsFactors = FALSE)

  conn <- dbConnect(RMySQL::MySQL(), dbname = "test")
  dbRemoveTable(conn, "mydf")
  dbWriteTable(conn, name = "mydf", value = myDF)

  expect_equal(dbReadTable(conn, "mydf"), myDF)

  dbRemoveTable(conn, "mydf")
  dbDisconnect(conn)
})

