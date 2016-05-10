context("last_cached_at")

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
    testthatsomemore::pretend_now_is("1 minute from now", {
      cached_fcn(key = ids, force. = TRUE)
    })
    new_sample_rows <- DBI::dbGetQuery(test_con, paste0("SELECT * FROM ", shard))
    expect_equal(NROW(sample_rows), NROW(new_sample_rows))
    expect_false(identical(sample_rows$last_cached_at, new_sample_rows$last_cached_at))
    expect_true(all(sample_rows$last_cached_at < new_sample_rows$last_cached_at))
  })

  describe("when last_cached_at does not yet exist in shard", {
    db_test_that("it adds the last_cached_at column with new data", {
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix)
      cached_fcn(key = 5:1,  model_version, type)

      shard_name <- cached_fcn(key = 5:1,  model_version, type, dry. = TRUE)$shard_names[[1]]

      # Drop last_cached_at column to mimic tables in older versions of cmiyc
      DBI::dbGetQuery(test_con, paste("alter table", shard_name, "drop column", "last_cached_at"))

      df <- DBI::dbGetQuery(test_con, paste("select * from ", shard_name, "limit 1"))
      expect_false("last_cached_at" %in% names(df))

      # Cache new data
      cached_fcn(key = 5:10,  model_version, type)

      # Verify that the last_cached_at column exists
      df <- DBI::dbGetQuery(test_con, paste("select * from ", shard_name, "limit 1"))
      expect_true("last_cached_at" %in% names(df))
    })

    db_test_that("it adds the last_cached_at column with forced data", {
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix)
      cached_fcn(key = 5:1,  model_version, type)

      shard_name <- cached_fcn(key = 5:1,  model_version, type, dry. = TRUE)$shard_names[[1]]

      # Drop last_cached_at column to mimic tables in older versions of cmiyc
      DBI::dbGetQuery(test_con, paste("alter table", shard_name, "drop column", "last_cached_at"))

      df <- DBI::dbGetQuery(test_con, paste("select * from ", shard_name, "limit 1"))
      expect_false("last_cached_at" %in% names(df))

      # 1. Unforced cache retrival doesn't change anything
      cached_fcn(key = 5:1,  model_version, type)
      df <- DBI::dbGetQuery(test_con, paste("select * from ", shard_name, "limit 1"))
      expect_false("last_cached_at" %in% names(df))

      # 2. Force cache data adds last_cached_at column
      cached_fcn(key = 5:1,  model_version, type, force. = TRUE)
      df <- DBI::dbGetQuery(test_con, paste("select * from ", shard_name, "limit 1"))
      expect_true("last_cached_at" %in% names(df))
    })
  })
})
