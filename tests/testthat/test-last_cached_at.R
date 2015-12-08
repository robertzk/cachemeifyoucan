context("last_cached_at")

library(DBI)
library(dbtest)
library(testthatsomemore)

describe("last_cached_at", {
  db_test_that("last_cached_at is added to table but not returned in data.frame", {
    cached_fcn <- cache(batch_data, key = c(key = "id"), c("version"), con = test_con, prefix = "batch_data")

    ids <- 1:5

    ref_df <- batch_data(ids)
    cache_df <- cached_fcn(key = ids)
    expect_equal(ref_df, cache_df)

    # Verify that the returned data.frame doesn't include `last_cached_at`
    expect_false(exists("last_cached_at", cache_df))

    # Verify that the shard table has a `last_cached_at` column
    shard <- cached_fcn(key = ids, dry. = TRUE)$shard_names[[1]]
    sample_rows <- DBI::dbGetQuery(test_con, paste0("SELECT * FROM ", shard))
    expect_true(exists("last_cached_at", sample_rows))

    # Verify that the last_cached_at doesn't update when new data isn't pulled
    cached_fcn(key = ids)
    new_sample_rows <- DBI::dbGetQuery(test_con, paste0("SELECT * FROM ", shard))
    expect_equal(NROW(sample_rows), NROW(new_sample_rows))
    expect_true(identical(sample_rows$last_cached_at, new_sample_rows$last_cached_at))

    # Verify that the last_cached_at updates when `force. = TRUE` and time passes
    Sys.sleep(1)
    cached_fcn(key = ids, force. = TRUE)
    new_sample_rows <- DBI::dbGetQuery(test_con, paste0("SELECT * FROM ", shard))
    expect_equal(NROW(sample_rows), NROW(new_sample_rows))
    expect_false(identical(sample_rows$last_cached_at, new_sample_rows$last_cached_at))
    expect_true(all(sample_rows$last_cached_at < new_sample_rows$last_cached_at))
  })
})
