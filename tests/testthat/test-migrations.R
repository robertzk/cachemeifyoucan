context('cache migrations')

library(DBI)
library(dbtest)
library(testthatsomemore)

describe("update_cache_salt", {

  batch_data_locale <- function(..., locale = "US") {
    df <- batch_data(...)
    df$locale <- locale
    Sys.sleep(0.8)
    df
  }

  db_test_that('updating_cache_salt creates new shards', {
    ids <- 1:5
    model_version <- "test_data"
    locale <- "US"

    prefix <- "batch_data_locale"

    old_cache_fn <- cache(batch_data_locale, key = c(key = "id"), c("model_version"), con = test_con, prefix = prefix)
    new_cache_fn <- cache(batch_data_locale, key = c(key = "id"), c("model_version", "locale"), con = test_con, prefix = prefix)

    ref_df <- batch_data_locale(key = ids, model_version = model_version, locale = locale)

    # Cache some rows
    old_cache_fn(key = ids, model_version = model_version, locale = locale)
    takes_less_than(0.5)(old_df <- old_cache_fn(key = ids, model_version = model_version, locale = locale))
    expect_equal(ref_df, old_df)

    # Migrate caches over to new salt
    old_salt <- list(model_version = model_version)
    new_salt <- list(model_version = model_version, locale = locale)
    update_cache_salt(test_con, prefix, old_salt, new_salt)

    # Verify that the data is fetched from new cache quickly
    takes_less_than(0.5)(new_df <- new_cache_fn(key = ids, model_version = model_version, locale = locale))
    expect_equal(ref_df, new_df)

    # Verify that the data is still fetched from old cache quickly
    takes_less_than(0.5)(old_df <- old_cache_fn(key = ids, model_version = model_version, locale = locale))
    expect_equal(ref_df, old_df)
  })
})
