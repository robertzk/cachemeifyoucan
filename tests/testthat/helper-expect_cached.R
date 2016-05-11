expect_almost_equal <- function(..., tolerance = 1e-5) {
  expect_equal(..., tolerance = tolerance)
}

return_ids <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, stringsAsFactors = FALSE)
}

return_nas <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = rep(NA_character_, length(key)), stringsAsFactors = FALSE)
}

return_falses <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = rep("FALSE", length(key)), stringsAsFactors = FALSE)
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
  data.frame(id = key, data = cycle_vector_to_length(c("pizza", "potato", "apple", "banana"), length(key)), stringsAsFactors = FALSE)
}

return_mix_na <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = ifelse(key %% 2 == 0, "value", NA), stringsAsFactors = FALSE)
}

return_mix_false <- function(key, model_version = "model_test", type = "record_id") {
  data.frame(id = key, data = ifelse(key %% 2 == 0, "value", FALSE), stringsAsFactors = FALSE)
}

expect_cached <- function(expr, no_check = FALSE, fn = batch_data, ...) {
  id_key <- "id"
  cached_fcn <- cache(fn,
    key = c(key = id_key), c("model_version", "type"),
    con = test_con, prefix = prefix, ...)
  eval(substitute(expr), envir = environment())

  shards <- cachemeifyoucan:::get_shards_for_table(test_con,
    cachemeifyoucan:::table_name(prefix,
      list(model_version = model_version, type = type)))[[1]]

  if (identical(no_check, FALSE)) {
    lst <- lapply(shards, function(shard) {
      dff <- dbReadTable(test_con, shard)
      db2df(dff, test_con, id_key)
    })
    df_db <- cachemeifyoucan:::merge2(lst, id_key)
    expect_almost_equal(df_db, df_ref)
  }
  if (exists("df_cached", envir = environment(), inherits = FALSE)) {
    expect_almost_equal(df_cached, df_ref)
  }
}
