with_connection <- function(conn, expr) {
  assign("conn", conn, envir = parent.frame())
  on.exit(dbDisconnect(conn), add = TRUE)
  eval.parent(substitute(expr))
}

expect_almost_equal <- function(..., tolerance = 1e-5) expect_equal(..., tolerance = tolerance)

expect_cached <- function(expr) {
  with_connection(dbconn(), {
    lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
    cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = conn, prefix = prefix)
    eval(substitute(expr), envir = environment())

    shards <- cachemeifyoucan:::get_shards_for_table(conn, cachemeifyoucan:::table_name(prefix, list(model_version = model_version, type = type)))[[1]]
    lst <- lapply(shards, function(shard) {
      dff <- dbReadTable(conn, shard)
      dff <- dff[colnames(dff) != 'id']
      colnames(dff) <- cachemeifyoucan:::translate_column_names(colnames(dff), conn)
      dff
    })
    df_db <- cachemeifyoucan:::merge2(lst, "id")

    if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
      expect_almost_equal(df_db, df_ref)
    }
    if (exists('df_cached', envir = environment(), inherits = FALSE)) {
      expect_almost_equal(df_cached, df_ref)
    }
  })
}
