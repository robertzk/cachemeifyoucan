context('cachemeifyoucan:batch_data')
library(RPostgreSQL)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
version <- "default/en-US/2.2.1"
dbconn <- DBI::dbConnect(dbDriver("PostgreSQL"), "robk")

test_that('Test caching actually works for avant::batch_data', {  
  return() # Comment out for now
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  # key loan_ids does not match database column loan_id so a quick hack to do the testing
  my_batch_data <- function(loan_id, version, ...) avant::batch_data(loan_id, version, ...)
  df_ref <- cachemeifyoucan::error_fn(my_batch_data(c(32835,32836), version))
  cached_fcn <- cache(my_batch_data, prefix, key = "loan_id", salt = "version", con = dbconn)
  cached_fcn(c(32835,32836), version)
  df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, version)), 
    dbconn, "loan_id")
  col_names <- sort(colnames(df_ref))
  expect_equal(dplyr::arrange(df_db[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
  cached_fcn(c(32835,32836), version)
  df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, version)), 
    dbconn, "loan_id")
  expect_equal(dplyr::arrange(df_db[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
})
