context('uncached function')
library(DBI)
library(testthatsomemore)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
describe("uncached function", {

  test_that('calling the cached function for the first time populated a new table', {
    # First remove all tables in the local database.
    expect_cached({
      df_ref <- batch_data(1:5)
      df_cached <- cached_fcn(key = 1:5, model_version, type)
    })
  })

  test_that('uncached function returns identical function for a non-cached func', {
    f <- function(...) TRUE
    expect_equal(uncached(f), f)
  })

  test_that('uncaching batch data is also a cached function, but with force', {
    expect_cached({
      df_ref <- batch_data(1:5)
      df_cached <- uncached(cached_fcn)(key = 1:5, model_version, type)
    })
  })

  test_that('uncached invalidates cache', {
    with_connection(dbconn(), {
      # clean up
      lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
      # set up some initial data
      df <- data.frame(id = c(1,2,3,4,5), value = c(T,T,F,F,F))
      func <- function(id, value) if(missing(value)) df[id, ] else df[id,]
      c_func <- cache(func, key = "id", salt = "value", con = conn, prefix = prefix)
      # overwrite func!
      func <- function(id, value) if(missing(value)) data.frame(id = id, value = NA) else data.frame(id = id, value = value)
      cc_func <- cache(func, key = "id", salt = "value", con = conn, prefix = prefix)

      # cached version works as expected
      expect_equal(df[1,], c_func(1, "test"))
      # both pre- and after-cache
      expect_equal(as.character(df[1,]$value), c_func(1, "test")$value)
      # cc_func will also return the row from df because it'll think it's cached
      expect_equal(as.character(df[1,]$value), cc_func(1, "test")$value)
      # but now we will call uncached! and we should overwrite the db
      expect_equal(data.frame(id = 1, value = "test"), uncached(cc_func)(1, "test"))
      # and now the c_func will also read the new value from cache, because we've used force
      expect_equal("test", c_func(1, "test")$value)

      # clean up again
      lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
    })
  })
})
