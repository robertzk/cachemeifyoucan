context('cache')
library(RPostgreSQL)
library(testthatsomemore)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
salt <- "model_test"
seed <- 29
dbconn <- dbConnect(dbDriver("PostgreSQL"), "feiye")

db2df <- function(df) {
  df$loan_id <- NULL
  colnames(df) <- translate_column_names(colnames(df), dbconn)
  df
}

my_batch_data <- function(loan_ids, version = "model_test", ..., 
  verbose = TRUE, strict = TRUE, cache = TRUE, .depth = 0) {
  db2df(dbReadTable(dbconn, table_name(version)))
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
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]))
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
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]))
  expect_equal(df_db, df_ref)
}))

#test_that('Test appending partially overlapped table with missing value', 
#package_stub("avant", "batch_data", my_batch_data,
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  my_fcn <- function(loan_id, key = "loan_id") {
#    set.seed(seed)
#    df <- data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
#    df[nrow(df), ncol(df)] <- NULL
#    df
#  }
#  df_ref <- my_fcn(1:5)
#  print(df_ref)
#  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
#  #cached_fcn(loan_id = 5, key = "loan_id")
#  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]))
#  print(df_db)
#  #expect_equal(df_db, df_ref)
#}))

#test_that('Test insert new table and append some overlapping', 
#package_stub("avant", "batch_data", my_batch_data,
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  my_fcn <- function(loan_id, key = "loan_id") {
#   set.seed(seed)
#   data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
#  }
#  print(my_fcn(1:5))
#  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
#  cached_fcn(loan_id = 1:5, key = "loan_id")
#  print(db2df(dbReadTable(dbconn, dbListTables(dbconn)[2])))
#  cached_fcn(loan_id = 5, key = "loan_id")
#  print(db2df(dbReadTable(dbconn, dbListTables(dbconn)[2])))
#  cached_fcn(loan_id = 11:15, key = "loan_id")
#  print(db2df(dbReadTable(dbconn, dbListTables(dbconn)[2])))
#  assert(TRUE) 
#}))
