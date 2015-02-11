expect_cached <- function(expr) {
  dbconn <- get("dbconn", envir = parent.frame())
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = dbconn, prefix = prefix)
  eval(substitute(expr), envir = environment())
  df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, list(model_version = model_version, type = type))), dbconn, "id")
  if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
    expect_equal(df_db, df_ref)
  }
  if (exists('df_cached', envir = environment(), inherits = FALSE)) {
    expect_equal(df_cached, df_ref)
  }
}
