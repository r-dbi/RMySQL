context("dbWriteTable")

# test_that("can't override existing table with default options", {
#   con <- mysqlDefault()
#
#   x <- data.frame(col1 = 1:10, col2 = letters[1:10])
#   dbWriteTable(con, "t1", x, temporary = TRUE)
#   expect_error(dbWriteTable(con, "t1", x), "exists in database")
#   dbDisconnect(con)
# })

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

test_that("commas in fields are preserved", {
  con <- mysqlDefault()

  df <- data.frame(
    x = c("ABC, Inc.","DEF Holdings"),
    stringsAsFactors = FALSE
  )
  dbWriteTable(con, "t1", df, overwrite = TRUE)
  expect_equal(dbReadTable(con, "t1"), df)

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

test_that("can round-trip utf-8", {
  angstrom <- enc2utf8("\u00e5")
  con <- mysqlDefault()

  dbGetQuery(con, "CREATE TEMPORARY TABLE test1 (x TEXT)")
  # charToRaw(angstrom)
  dbGetQuery(con, "INSERT INTO test1 VALUES (0xc3a5)")

  expect_equal(dbGetQuery(con, "SELECT * FROM test1")$x, angstrom)
  expect_equal(dbGetQuery(con, "SELECT * FROM test1 WHERE x = 0xc3a5")$x,
    angstrom)

  dbDisconnect(con)
})

test_that("can read file from disk", {
  con <- mysqlDefault()

  expected <- data.frame(
    a = c(1:3, NA),
    b = c("x", "y", "z", "E"),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "dat", "dat-n.txt", sep = "|", eol = "\n", temporary = TRUE)
  expect_equal(dbReadTable(con, "dat"), expected)

  dbDisconnect(con)
})
