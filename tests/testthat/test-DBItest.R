DBItest::make_context(MySQL(), list(dbname = "test"))
DBItest::test_all(c(
  "constructor_strict",    # relaxed version of constructor check still active
  "show",                  # rstats-db/RPostgres#49
  "get_info",              # to be discussed
  "data_logical",          # not an error: no logical data type
  "data_64_bit",           # rstats-db/RPostgres#51
  "data_character",        # #93
  "data_time",             # #95
  "data_time_parens",      # #95
  "data_timestamp_utc",    # syntax not supported
  "data_timestamp_parens", # syntax not supported
  "roundtrip_quotes",      # #101
  NULL
))
