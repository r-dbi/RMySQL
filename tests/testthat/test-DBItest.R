DBItest::make_context(MySQL(), list(dbname = "test"))
DBItest::test_all(c(
  "constructor_strict",    # relaxed version of constructor check still active
  "show",                  # rstats-db/RPostgres#49
  "get_info",              # to be discussed
  "data_logical",          # not an error: no logical data type
  "data_logical_null",     # not an error: no logical data type
  "data_logical_int",      # not an error: no logical data type
  "data_logical_int_null", # not an error: no logical data type
  "data_64_bit",           # rstats-db/RPostgres#51
  "data_64_bit_null",      # rstats-db/RPostgres#51
  "data_character",        # #93
  "data_character_null",   # #93
  "data_time",             # #95
  "data_time_null",        # #95
  "data_time_parens",      # #95
  "data_time_parens_null", # #95
  "data_timestamp_utc",    # syntax not supported
  "data_timestamp_utc_null", # syntax not supported
  "data_timestamp_parens", # syntax not supported
  "data_timestamp_parens_null", # syntax not supported
  "roundtrip_quotes",      # #101
  "roundtrip_logical",     # not an error: no logical data type
  "roundtrip_numeric_special", # #105
  "roundtrip_64_bit",      # rstats-db/DBI#48
  "roundtrip_character",   # #93
  "roundtrip_timestamp",   # #104
  NULL
))
