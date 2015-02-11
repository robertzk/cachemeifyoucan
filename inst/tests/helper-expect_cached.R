with_connection <- function(conn, expr) {
  assign("conn", conn, envir = parent.frame())
  on.exit(dbDisconnect(conn), add = TRUE)
  eval.parent(substitute(expr))
}

expect_cached <- function(expr) {
  with_connection(dbconn(), {
    lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
    cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = conn, prefix = prefix)
    eval(substitute(expr), envir = environment())
    df_db <- db2df(dbReadTable(conn, cachemeifyoucan:::table_name(prefix, list(model_version = model_version, type = type))), conn, "id")
    if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
      expect_equal(df_db, df_ref)
    }
    if (exists('df_cached', envir = environment(), inherits = FALSE)) {
      expect_equal(df_cached, df_ref)
    }
  })
}

