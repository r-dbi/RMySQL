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

test_that("ignore duplicate entries if `onexists=replace`", {
  con <- mysqlDefault()

  x <- data.frame(col1 = 1:10, col2 = letters[1:10])

  dbWriteTable(con, "t1", x, overwrite = TRUE)
  dbGetQuery(con, "ALTER TABLE t1 ADD PRIMARY KEY (`col1`)")

  x2 <- data.frame(col1 = 1, col2 = letters[2])

  dbWriteTable(con, "t1", x2, append = TRUE, onduplicate = "ignore")
  expect_equal(dbGetQuery(con, "SELECT * FROM t1 WHERE col1 = 1")$col2,
               letters[1])

  dbWriteTable(con, "t1", x2, append = TRUE, onduplicate = "replace")
  expect_equal(dbGetQuery(con, "SELECT * FROM t1 WHERE col1 = 1")$col2,
               letters[2])

  dbDisconnect(con)
})

# Available only in MySQL
test_that("can read file from disk", {
  con <- mysqlDefault()

  expected <- data.frame(
    a = c(1:3, NA),
    b = c("x", "y", "z", "E"),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "dat", "dat-n.bin", sep = "|", eol = "\n",
               temporary = TRUE, overwrite = TRUE)
  expect_equal(dbReadTable(con, "dat"), expected)

  dbDisconnect(con)
})
