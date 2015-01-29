context('cachemeifyoucan:batch_data')
library(RPostgreSQL)
library(digest)
library(avant)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
salt <- "model_test"
dbconn <- dbConnect(dbDriver("PostgreSQL"), "feiye")

test_that('Test caching actually works for avant::batch_data', 
{  
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_batch_data <- function(..., key = "loan_id") do.call(batch_data, list(...))
  df_ref <- cachemeifyoucan::error_fn(my_batch_data(c(32835,32836), 'default/en-US/2.2.1'))
  cached_fcn <- cache(my_batch_data, prefix, salt, key = "loan_id")
  df_first_cached <- cached_fcn(loan_id = c(32835,32836), 'default/en-US/2.2.1', con = dbconn)
  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "loan_id")
  col_names <- sort(colnames(df_ref))
  expect_equal(dplyr::arrange(df_first_cached[, col_names], loan_id), 
    dplyr::arrange(df_db[, col_names], loan_id))
  expect_equal(dplyr::arrange(df_first_cached[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
  expect_equal(dplyr::arrange(df_db[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
  df_second_cached <- cached_fcn(loan_id = c(32835,32836), 'default/en-US/2.2.1', con = dbconn)
  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "loan_id")
  expect_equal(dplyr::arrange(df_second_cached[, col_names], loan_id), 
    dplyr::arrange(df_db[, col_names], loan_id))
  expect_equal(dplyr::arrange(df_second_cached[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
  expect_equal(dplyr::arrange(df_db[, col_names], loan_id), 
    dplyr::arrange(df_ref[, col_names], loan_id))
})
