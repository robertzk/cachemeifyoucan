expect_almost_equal <- function(..., tolerance = 1e-5) expect_equal(..., tolerance = tolerance)

expect_cached <- function(expr, no_check = FALSE) {
  dbtest::with_test_db({
    lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
    cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix)
    eval(substitute(expr), envir = environment())

    shards <- cachemeifyoucan:::get_shards_for_table(test_con, cachemeifyoucan:::table_name(prefix, list(model_version = model_version, type = type)))[[1]]
    lst <- lapply(shards, function(shard) {
      dff <- dbReadTable(test_con, shard)
      dff <- dff[colnames(dff) != 'id']
      colnames(dff) <- cachemeifyoucan:::translate_column_names(colnames(dff), test_con)
      dff
    })
    df_db <- cachemeifyoucan:::merge2(lst, "id")

    if (identical(no_check, FALSE)) {
      expect_almost_equal(df_db, df_ref)
    }
    if (exists('df_cached', envir = environment(), inherits = FALSE)) {
      expect_almost_equal(df_cached, df_ref)
    }
  })
}
