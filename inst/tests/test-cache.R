context('cache function')
library(RPostgreSQL)
library(digest)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
local({
  prefix        <- "version"
  model_version <- "model_test"
  type          <- "record_id"
  dbconn        <- DBI::dbConnect(dbDriver("PostgreSQL"), "robk")

  # This function gives deterministic output for each id.
  batch_data <- function(id, model_version = "model_test", type = "record_id", 
    switch = FALSE, flip = integer(0), ...) {
    original <- Reduce(rbind, lapply(id, function(id) {
      seed <- as.integer(paste0("0x", substr(digest(paste(id, model_version, type)), 1, 6)))
      set.seed(seed)
      data.frame(id = id, x = runif(1), y = rnorm(1))}))
    ret <- original
    if (switch) ret$y <- NA
    if (switch && length(flip) >= 1)
      ret[flip, "y"] <- original[flip, "y"]
    ret
  }

  without_rownames <- function(df) { row.names(df) <- NULL; df }

  expect_cached <- function(expr) {
    lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
    cached_fcn <- cache(batch_data, key = "id", c("model_version", "type"), con = dbconn, prefix = prefix)
    eval(substitute(expr), envir = environment())
    df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, c(model_version, type))), dbconn, "id")
    if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
      expect_equal(df_db, df_ref)
    }
    if (exists('df_cached', envir = environment(), inherits = FALSE)) {
      expect_equal(df_cached, df_ref)
    }
  }

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
      expect_equal(without_rownames(df_ref[5:1, ]),
                   without_rownames(cached_fcn(id = 5:1, model_version, type)))
      no_check <- TRUE
    })
  })

  RPostgreSQL::postgresqlCloseConnection(dbconn)
})

