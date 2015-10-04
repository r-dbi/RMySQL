DBItest::make_context(MySQL(), list(dbname = "test"))
DBItest::test_all(c("constructor_strict", "show", "get_info", "invalid_query", "data_logical"))
