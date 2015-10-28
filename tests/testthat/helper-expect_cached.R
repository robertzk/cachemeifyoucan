expect_almost_equal <- function(..., tolerance = 1e-5) expect_equal(..., tolerance = tolerance)

return_nas <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = rep(NA_character_, length(key)))
}

#' Converts a vector to a particular length by cycling it.
#' @param v vector. The vector to cycle.
#' @param len integer. The length to make it.
cycle_vector_to_length <- function(v, len) {
  stopifnot(is.numeric(len) && len > 0)
  len <- as.integer(len)
  v <- rep(v, len)
  length(v) <- len
  v
}

return_foods <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = cycle_vector_to_length(c("pizza", "potato", "apple", "banana"), length(key)))
}

expect_cached <- function(expr, no_check = FALSE, fn = batch_data) {
  cached_fcn <- cache(fn, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix)
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
}
