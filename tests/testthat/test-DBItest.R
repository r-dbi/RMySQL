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
  "data_raw",              # not an error: can't cast to blob type
  "data_raw_null",         # not an error: can't cast to blob type
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
  "get_exception",         # #106
  "bind_logical_positional_qm", # not an error: no logical data type
  "bind_character_positional_qm", # #93
  "bind_timestamp_lt_positional_qm", # #110
  "bind_raw_positional_qm", # #110
  "bind_.*_named_.*",      # not an error: named binding not supported
  NULL
))
