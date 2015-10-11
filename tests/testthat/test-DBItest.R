DBItest::make_context(MySQL(), list(dbname = "test"))
DBItest::test_all(c(
  "constructor_strict",    # relaxed version of constructor check still active
  "show",                  # rstats-db/RPostgres#49
  "get_info",              # to be discussed
  "invalid_query",         # #91
  "data_logical",          # not an error: no logical data type
  "data_64_bit",           # rstats-db/RPostgres#51
  "data_character",        # #93
  "data_time",             # #94
  "data_time_parens",      # #94
  "data_timestamp_parens", # syntax not supported
  NULL
))
