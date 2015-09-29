context('cache function')
library(DBI)
library(testthatsomemore)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
describe("cache function", {

  # test_that('calling the cached function for the first time populated a new table', {
  #   # First remove all tables in the local database.
  #   expect_cached({
  #     df_ref <- batch_data(1:5)
  #     df_cached <- cached_fcn(key = 1:5, model_version, type)
  #   })
  # })
  #
  # test_that('We can cache big tables', {
  #   with_connection(dbconn(), {
  #     lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
  #     cached_fcn <- cache(batch_huge_data, key = c(key = "id"), c("version"), con = conn, prefix = "huge_data")
  #     lapply(list(1:10, 1:20), function(ids) {
  #       # Populate the cache and make sure that the results are equal
  #       expect_equal(dim(bd <- batch_huge_data(ids)), dim(cached_fcn(ids)))
  #       # Make sure we're reading from cache and it's fast now!
  #       takes_less_than(1)(tmp <- cached_fcn(ids))
  #       # And the results are still correct
  #       expect_equal(dim(bd), dim(tmp))
  #     })
  #     # And now everything is so cached
  #     takes_less_than(1)(tmp <- cached_fcn(1:20))
  #   })
  # })
  #
  # test_that('retrieving partial result from cache works', {
  #   expect_cached({
  #     df_ref <- batch_data(1:5)
  #     cached_fcn(key = 1:5, model_version, type)
  #     expect_almost_equal(df_ref[1, ], cached_fcn(key = 1, model_version, type))
  #   })
  # })
  #
  # test_that('attempting to populate a new row with a different value fails due to cache hit', {
  #   expect_cached({
  #     df_ref <- batch_data(1:5, switch = TRUE, flip = 4:5)
  #     cached_fcn(key = 1:5, model_version, type, switch = TRUE, flip = 4:5)
  #     cached_fcn(key = 4, model_version, type)
  #     cached_df <- cached_fcn(1:5, switch = TRUE, flip = 4:5)
  #   })
  # })
  #
  # test_that('appending partially overlapped table adds to cache', {
  #   expect_cached({
  #     df_ref <- batch_data(1:5, model_version, type, switch = TRUE, flip = 1)
  #     df_ref <- rbind(df_ref, batch_data(6, model_version, type))
  #     cached_fcn(key = 1:5, model_version, type, switch = TRUE, flip = 1)
  #     cached_fcn(key = 5:6, model_version, type)
  #   })
  # })
  #
  # test_that('re-arranging in the correct order happens when using the cache', {
  #   expect_cached({
  #     df_ref <- batch_data(1:5, model_version, type)
  #     cached_fcn(key = 1:5, model_version, type)
  #     expect_almost_equal(without_rownames(df_ref[5:1, ]),
  #                  without_rownames(cached_fcn(key = 5:1, model_version, type)))
  #     no_check <- TRUE
  #   })
  # })
  #
  # test_that('re-arranging in the correct order happens when using the cache with partially new results', {
  #   expect_cached({
  #     df_ref <- batch_data(1:5, model_version, type)
  #     cached_fcn(key = 1:3, model_version, type)
  #     expect_almost_equal(without_rownames(df_ref[5:1, ]),
  #                  without_rownames(cached_fcn(key = 5:1, model_version, type)))
  #     no_check <- TRUE
  #   })
  # })

  test_that('non-numeric primary keys are supported', {
    expect_cached({
      df_ref <- batch_data(letters[1:5])
      cached_fcn(key = letters[1:5], model_version, type)
      expect_almost_equal(df_ref[1, ], cached_fcn(key = 'a', model_version, type))
    })
  })

  test_that('the force. parameter triggers cache re-population', {
    # First remove all tables in the local database.
    expect_cached({
      df_ref <- batch_data(1:5)
      package_stub("cachemeifyoucan", "try_write_data_safely", function(...) {
        stop("Caching layer should not be used")
      }, {
        expect_error(df_cached <- cached_fcn(key = 1:5, model_version, type, force. = FALSE),
                     "Caching layer should not be used")
      })
      df_cached <- cached_fcn(key = 1:5, model_version, type, force. = TRUE)
    })
  })

})
