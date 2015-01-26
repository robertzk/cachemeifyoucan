context('cachemeifyoucan')
library(RPostgreSQL)
library(testthatsomemore)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
salt <- "model_test"
seed <- 29
dbconn <- dbConnect(dbDriver("PostgreSQL"), "feiye")

my_batch_data <- function(loan_ids, version = "model_test", ..., 
  verbose = TRUE, strict = TRUE, cache = TRUE, .depth = 0) {
  db2df(dbReadTable(dbconn, table_name(version)), dbconn)
}

set_NULL <- function(loan_id) {
  dbSendQuery(dbconn, 
    paste0("update version_d9eccf5ac458b7294c552cbb9166ce7e set 
      cb25dd2c01c3954a0cdc3fa18ac4bcfc5 = NULL where loan_id = ", loan_id))
}

test_that('Test inserting new table', 
package_stub("avant", "batch_data", my_batch_data,
{  
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id") {
   set.seed(seed)
   data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
  df_cached <- cached_fcn(loan_id = 1:5, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  expect_equal(df_cached, df_ref)
  expect_equal(df_db, df_ref)
}))

test_that('Test appending partially overlapped table', 
package_stub("avant", "batch_data", my_batch_data,
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id") {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id")
  cached_fcn(loan_id = 5, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  expect_equal(df_db, df_ref)
}))

test_that('Test appending fully overlapped table with missing value', 
package_stub("avant", "batch_data", my_batch_data,
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id") {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  df_ref[5, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  set_NULL(5)
  cached_fcn(loan_id = 5, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  expect_equal(df_db, df_ref)
}))

test_that('Test appending partially overlapped table with missing value', 
package_stub("avant", "batch_data", my_batch_data,
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id") {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:6)
  df_ref[5, 2] <- df_ref[6, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  set_NULL(5)
  cached_fcn(loan_id = 5:6, key = "loan_id")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn)
  expect_equal(dplyr::arrange(df_db, loan_id), dplyr::arrange(df_ref, loan_id))
}))
