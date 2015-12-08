expect_almost_equal <- function(..., tolerance = 1e-5) expect_equal(..., tolerance = tolerance)

expect_cached <- function(expr, no_check = FALSE) {
  id_key <- "id"
  cached_fcn <- cache(batch_data, key = c(key = id_key), c("model_version", "type"), con = test_con, prefix = prefix)
  eval(substitute(expr), envir = environment())

  shards <- cachemeifyoucan:::get_shards_for_table(test_con, cachemeifyoucan:::table_name(prefix, list(model_version = model_version, type = type)))[[1]]
  lst <- lapply(shards, function(shard) {
    dff <- dbReadTable(test_con, shard)
    db2df(dff, test_con, id_key)
  })
  df_db <- cachemeifyoucan:::merge2(lst, id_key)

  if (identical(no_check, FALSE)) {
    expect_almost_equal(df_db, df_ref)
  }
  if (exists('df_cached', envir = environment(), inherits = FALSE)) {
    expect_almost_equal(df_cached, df_ref)
  }
}
