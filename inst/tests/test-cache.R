context('cache function')
library(DBI)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
describe("cache function", {
  dbconn <- db_connection("database.yml", "cache")

  test_that('calling the cached function for the first time populated a new table', {  
    # First remove all tables in the local database.
    expect_cached({
      df_ref <- batch_data(1:5)
      df_cached <- cached_fcn(id = 1:5, model_version, type)
    })
  })

  test_that('retrieving partial result from cache works', { 
    expect_cached({
      df_ref <- batch_data(1:5)
      cached_fcn(id = 1:5, model_version, type)
      expect_equal(df_ref[1, ], cached_fcn(id = 1, model_version, type))
    })
  })

  test_that('attempting to populate a new row with a different value fails due to cache hit', { 
    expect_cached({
      df_ref <- batch_data(1:5, switch = TRUE, flip = 4:5)
      cached_fcn(id = 1:5, model_version, type, switch = TRUE, flip = 4:5)
      cached_fcn(id = 4, model_version, type)
      cached_df <- cached_fcn(1:5, switch = TRUE, flip = 4:5)
    })
  })

  test_that('appending partially overlapped table adds to cache', { 
    expect_cached({
      df_ref <- batch_data(1:5, model_version, type, switch = TRUE, flip = 1)
      df_ref <- rbind(df_ref, batch_data(6, model_version, type))
      cached_fcn(id = 1:5, model_version, type, switch = TRUE, flip = 1)
      cached_fcn(id = 5:6, model_version, type)
    })
  })

  test_that('re-arranging in the correct order happens when using the cache', {
    expect_cached({
      df_ref <- batch_data(1:5, model_version, type)
      cached_fcn(id = 1:5, model_version, type)
      expect_equal(without_rownames(df_ref[5:1, ]),
                   without_rownames(cached_fcn(id = 5:1, model_version, type)))
      no_check <- TRUE
    })
  })

  test_that('re-arranging in the correct order happens when using the cache with partially new results', {
    expect_cached({
      df_ref <- batch_data(1:5, model_version, type)
      cached_fcn(id = 1:3, model_version, type)
      expect_equal(without_rownames(df_ref[5:1, ]),
                   without_rownames(cached_fcn(id = 5:1, model_version, type)))
      no_check <- TRUE
    })
  })

  dbDisconnect(dbconn)
})
