context('uncached function')

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
describe("uncached function", {

  db_test_that('calling the cached function for the first time populated a new table', {
    # First remove all tables in the local database.
    expect_cached({
      df_ref <- batch_data(1:5)
      df_cached <- cached_fcn(key = 1:5, model_version, type)
    })
  })

  db_test_that('uncached function returns identical function for a non-cached func', {
    f <- function(...) TRUE
    expect_equal(uncached(f), f)
  })

  db_test_that('uncached undoes caching', {
    expect_cached({
      df_ref <- batch_data(1:5)
      df_cached <- cached_fcn(key = 1:5, model_version, type)
      expect_equal(uncached(cached_fcn), batch_data)
    })
  })
})
