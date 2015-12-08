context('cache metadata')

library(DBI)
library(dbtest)
library(testthatsomemore)

describe("track_cache_salt", {

  db_test_that("cache_metadata table existence", {
    # Verify that the cache_metadata is empty to start
    expect_false(DBI::dbExistsTable(test_con, CACHE_METADATA_TABLE))

    # Verify that an error is thrown if cache_metadata doesn't exist.
    expect_error(
      salts <- get_cache_table_salt(test_con, ref_table_name),
      "No cache_metadata table found."
    )
  })

  db_test_that("calling track_cache_salt creates a cache_metadata entry", {
    ref_table_name <- "test_data_xyz"
    ref_salt <- list(source = "test")

    # Track ref_table_name
    track_cache_salt(test_con, ref_table_name, ref_salt)

    # Verify that the salt is saved accurately
    salt <- get_cache_table_salt(test_con, ref_table_name)[[1]]
    expect_identical(ref_salt, salt)

    # Verify that it errors when using a mismatched salt
    new_ref_salt <- list(source = "test", params = "bar")
    expect_error(
      track_cache_salt(test_con, ref_table_name, new_ref_salt),
      "Cache salt values don't match what was registered."
    )

    # Verify that the cache_metadata is empty for a new table
    new_ref_table_name <- "test_data_123"
    expect_warning(
      salts <- get_cache_table_salt(test_con, new_ref_table_name),
      "No matching entry found."
    )
    expect_null(salts)

    # Execute new_ref_table_name
    track_cache_salt(test_con, new_ref_table_name, new_ref_salt)

    # Verify that the new salt is saved accurately
    salt <- get_cache_table_salt(test_con, new_ref_table_name)[[1]]
    expect_identical(new_ref_salt, salt)

    ref_table_names <- c(ref_table_name, new_ref_table_name)
    salts <- get_cache_table_salt(test_con, ref_table_names)
    ref_combined_salts = setNames(list(ref_salt, new_ref_salt), ref_table_names)
    expect_identical(ref_combined_salts, salts)
  })


  db_test_that("calling the cached_fn creates a metadata entry", {
    ids <- 1:5
    test_version <- "test"

    cache_fn <- cache(batch_data, key = c(key = "id"), c("model_version"), con = test_con, prefix = "batch_data")
    ref_table_name = cache_fn(key = ids, model_version = test_version, dry. = TRUE)$table_name

    cache_fn(key = ids, model_version = test_version)
    ref_salt <- list(model_version = test_version)

    # Verify that the salt is saved accurately
    salt <- get_cache_table_salt(test_con, ref_table_name)[[1]]
    expect_identical(ref_salt, salt)
  })
})
