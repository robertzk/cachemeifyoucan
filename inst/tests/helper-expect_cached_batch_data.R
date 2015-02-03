expect_cached_batch_data <- function(expr) {
  dbconn <- get("dbconn", envir = parent.frame())
  prefix <- get("prefix", envir = parent.frame())
  model_version <- get("model_version", envir = parent.frame())
  type <- get("type", envir = parent.frame())
  batch_data <- get("batch_data", envir = parent.frame())
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  cached_fcn <- cache(batch_data, key = "loan_id", "version", con = dbconn, prefix = prefix)
  eval(substitute(expr), envir = environment())
  df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, list(version = model_version))), dbconn, "loan_id")
  if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
    expect_equal(df_db, df_ref)
  }
  if (exists('df_cached', envir = environment(), inherits = FALSE)) {
    expect_equal(df_cached, df_ref)
  }
}
