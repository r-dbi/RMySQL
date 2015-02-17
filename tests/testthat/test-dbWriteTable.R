context("dbWriteTable")

test_that("options work", {
  con <- mysqlDefault()

  expected <- data.frame(
    a = c(1:3, NA),
    b = c("x", "y", "z", "E"),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "dat", "dat-n.txt", sep = "|", eol = "\n", temporary = TRUE)
  expect_equal(dbReadTable(con, "dat"), expected)
})
