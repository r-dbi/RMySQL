DBItest::make_context(
  MySQL(),
  list(dbname = "test", username = "", password = ""),
  tweaks = DBItest::tweaks(
    constructor_relax_args = TRUE,
    placeholder_pattern = "?"
  ),
  name = "RMySQL")
