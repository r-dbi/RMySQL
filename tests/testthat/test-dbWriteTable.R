context("dbWriteTable")

# test_that("can't override existing table with default options", {
#   con <- mysqlDefault()
#
#   x <- data.frame(col1 = 1:10, col2 = letters[1:10])
#   dbWriteTable(con, "t1", x, temporary = TRUE)
#   expect_error(dbWriteTable(con, "t1", x), "exists in database")
#   dbDisconnect(con)
# })

# Not generic enough for DBItest
test_that("throws error if constraint violated", {
  con <- mysqlDefault()

  x <- data.frame(col1 = 1:10, col2 = letters[1:10])

  dbWriteTable(con, "t1", x, overwrite = TRUE)
  dbGetQuery(con, "CREATE UNIQUE INDEX t1_c1_c2_idx ON t1(col1, col2(1))")
  expect_error(dbWriteTable(con, "t1", x, append = TRUE),
    "Duplicate entry")

  dbDisconnect(con)
})

test_that("rownames preserved", {
  con <- mysqlDefault()

  df <- data.frame(x = 1:10)
  row.names(df) <- paste(letters[1:10], 1:10, sep="")

  dbWriteTable(con, "t1", df, overwrite = TRUE)
  t1 <- dbReadTable(con, "t1")
  expect_equal(rownames(t1), rownames(df))

  dbDisconnect(con)
})

test_that("can roundtrip special field names", {
  con <- mysqlDefault()

  local <- data.frame(x = 1:3, select = 1:3, `,` = 1:3, check.names = FALSE)
  dbWriteTable(con, "torture", local, overwrite = TRUE)
  remote <- dbReadTable(con, "torture", check.names = FALSE)

  expect_equal(local, remote)

  dbDisconnect(con)
})

test_that("can read file from disk", {
  con <- mysqlDefault()

  expected <- data.frame(
    a = c(1:3, NA),
    b = c("x", "y", "z", "E"),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "dat", "dat-n.txt", sep = "|", eol = "\n",
               temporary = TRUE, overwrite = TRUE)
  expect_equal(dbReadTable(con, "dat"), expected)

  dbDisconnect(con)
})

test_that("appending is error if table does not exist", {
  con <- mysqlDefault()

  df <- data.frame(
    str = letters[1:5],
    num = 1:5,
    stringsAsFactors = FALSE
  )
  dbWriteTable(con, "dat", df, overwrite = TRUE)

  expect_error(
    dbWriteTable(con, "dat", df, overwrite = FALSE, append = FALSE),
    "Table dat exists"
  )

  dbRemoveTable(con, "dat")
  dbDisconnect(con)
})

test_that("temporary tables work properly", {
  con <- mysqlDefault()

  df <- data.frame(
    str = letters[1:5],
    num = 1:5,
    stringsAsFactors = FALSE
  )
  dbWriteTable(con, "dat", df, temporary = TRUE, row.names = FALSE)
  res <- dbGetQuery(con, "SELECT * FROM dat")
  expect_equal(res, df)

  dbGetQuery(con, "DELETE FROM dat")
  expect_true(dbWriteTable(con, "dat", df, append = TRUE, row.names = FALSE))

  res <- dbGetQuery(con, "SELECT * FROM dat")
  expect_equal(res, df)

  expect_error(
    dbWriteTable(con, "dat", df, append = FALSE, row.names = FALSE),
    "Table dat exists"
  )

  dbDisconnect(con)
})
