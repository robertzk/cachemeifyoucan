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

batch_data <- function(loan_ids, version = "model_test", ...) {
  db2df(dbReadTable(dbconn, table_name(version)), dbconn, "loan_id")
}

set_NULL <- function(loan_id) {
  dbSendQuery(dbconn, 
    paste0("update version_d9eccf5ac458b7294c552cbb9166ce7e set 
      cb25dd2c01c3954a0cdc3fa18ac4bcfc5 = NULL where loan_id = ", loan_id))
}

test_that('Test inserting new table', 
{  
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id", ...) {
   set.seed(seed)
   data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, key = "loan_id")
  #browser()
  df_cached <- cached_fcn(loan_id = 1:5, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  expect_equal(df_cached, df_ref)
  expect_equal(df_db, df_ref)
})

test_that('Test appending partially overlapped table', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id", ...) {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, key = "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id", con = dbconn, batch_data = batch_data)
  cached_fcn(loan_id = 5, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  expect_equal(df_db, df_ref)
})

test_that('Test appending fully overlapped table with missing value', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id", ...) {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:5)
  df_ref[5, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  set_NULL(5)
  cached_fcn(loan_id = 5, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  expect_equal(df_db, df_ref)
})

test_that('Test appending partially overlapped table with missing value', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id", ...) {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:6)
  df_ref[5, 2] <- df_ref[6, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  set_NULL(5)
  cached_fcn(loan_id = 5:6, key = "loan_id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  expect_equal(dplyr::arrange(df_db, loan_id), dplyr::arrange(df_ref, loan_id))
})

test_that('Test appending partially overlapped table with missing value 2', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(loan_id, key = "loan_id", ...) {
    set.seed(seed)
    data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  df_ref <- my_fcn(1:6)
  df_ref[5, 2] <- df_ref[6, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id", con = dbconn, 
    batch_data = batch_data, .select = "column_test")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  set_NULL(5)
  cached_fcn(loan_id = 5:6, key = "loan_id", con = dbconn,
    batch_data = batch_data, .select = "column_test")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "loan_id")
  expect_equal(dplyr::arrange(df_db, loan_id), dplyr::arrange(df_ref, loan_id))
})
