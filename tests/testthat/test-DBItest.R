DBItest::test_all(c(
  # getting_started
  "constructor_strict",                         # relaxed version of constructor check still active

  # driver
  "get_info_driver",                            # rstats-db/RSQLite#117

  # connection
  "get_info_connection",                        # rstats-db/RSQLite#117
  "cannot_disconnect_twice",                    # TODO

  # result
  "cannot_clear_result_twice_.*",               #
  "fetch_n_bad",                                #
  "fetch_n_good_after_bad",                     #
  "fetch_no_return_value",                      #
  "get_query_empty_.*",                         # syntax not supported
  "get_query_n_.*",                             #
  "clear_result_return",                        # error: need to warn if closing result twice
  "data_logical",                               # not an error: no logical data type
  "data_logical_null_.*",                       # not an error: no logical data type
  "data_logical_int",                           # not an error: no logical data type
  "data_logical_int_null_.*",                   # not an error: no logical data type
  "data_64_bit_.*",                             # #77
  "data_character",                             # #93
  "data_character_null_.*",                     # #93
  "data_raw",                                   # not an error: can't cast to blob type
  "data_raw_null_.*",                           # not an error: can't cast to blob type
  "data_time",                                  # #95
  "data_time_null_.*",                          # #95
  "data_time_parens",                           # #95
  "data_time_parens_null_.*",                   # #95
  "data_timestamp",                             # #113
  "data_timestamp_null_.*",                     # #113

  # sql
  "quote_string.*",                             # #115
  "quote_identifier_vectorized",                #
  "list_fields",                                # #137
  "list_tables",                                #
  "append_table_new",                           #
  "roundtrip_quotes",                           # #101
  "roundtrip_numeric_special",                  # #105
  "roundtrip_64_bit",                           # rstats-db/DBI#48
  "roundtrip_character",                        # #93
  "roundtrip_character_native",                 # #93
  "roundtrip_factor",                           # #93
  "roundtrip_date",                             #
  "roundtrip_time",                             #
  "roundtrip_raw",                              # #111
  "roundtrip_timestamp",                        # #104
  "read_table_error",                           #
  "read_table_name",                            #
  "write_table_error",                          #
  "write_table_name",                           #
  "exists_table_error",                         #
  "exists_table_name",                          #
  "remove_table_name",                          #
  "remove_table_temporary",                     #

  # meta
  "get_exception",                              # #106
  "get_info_result",                            # rstats-db/DBI#55
  "bind_empty.*",                               # #116
  "bind_return_value.*",                        # #116
  "bind_multi_row.*",                           # #170
  "bind_logical.*",                             # not an error: no logical data type
  "bind_character.*",                           # #93
  "bind_timestamp_lt.*",                        # #110
  "bind_raw.*",                                 # #110
  "bind_.*_positional_dollar",                  # not an error: named binding not supported
  "bind_.*_named_.*",                           # not an error: named binding not supported

  # transactions
  "commit_without_begin",                       # 167
  "begin_begin",                                # 167

  # compliance
  "compliance",                                 # #112
  "ellipsis",                                   # #171
  "read_only",                                  # default connection is read-write
  NULL
))
