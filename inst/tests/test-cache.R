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

batch_data <- function(ids, version = "model_test", ...) {
  db2df(dbReadTable(dbconn, table_name(version)), dbconn, "id")
}

set_NULL <- function(id) {
  dbSendQuery(dbconn, 
    paste0("update version_d9eccf5ac458b7294c552cbb9166ce7e set 
      cb25dd2c01c3954a0cdc3fa18ac4bcfc5 = NULL where id = ", id))
}

test_that('Test inserting new table', 
{  
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(id, key = "id", ...) {
   set.seed(seed)
   data.frame("id" = id, "column_test" = rnorm(length(id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, key = "id")
  df_cached <- cached_fcn(id = 1:5, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  expect_equal(df_cached, df_ref)
  expect_equal(df_db, df_ref)
})

test_that('Test appending partially overlapped table', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(id, key = "id", ...) {
    set.seed(seed)
    data.frame("id" = id, "column_test" = rnorm(length(id)))
  }
  df_ref <- my_fcn(1:5)
  cached_fcn <- cache(my_fcn, prefix, salt, key = "id")
  cached_fcn(id = 1:5, key = "id", con = dbconn, batch_data = batch_data)
  cached_fcn(id = 5, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  expect_equal(df_db, df_ref)
})

test_that('Test appending fully overlapped table with missing value', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(id, key = "id", ...) {
    set.seed(seed)
    data.frame("id" = id, "column_test" = rnorm(length(id)))
  }
  df_ref <- my_fcn(1:5)
  df_ref[5, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "id")
  cached_fcn(id = 1:5, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  set_NULL(5)
  cached_fcn(id = 5, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  expect_equal(df_db, df_ref)
})

test_that('Test appending partially overlapped table with missing value', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(id, key = "id", ...) {
    set.seed(seed)
    data.frame("id" = id, "column_test" = rnorm(length(id)))
  }
  df_ref <- my_fcn(1:6)
  df_ref[5, 2] <- df_ref[6, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "id")
  cached_fcn(id = 1:5, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  set_NULL(5)
  cached_fcn(id = 5:6, key = "id", con = dbconn, batch_data = batch_data)
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  expect_equal(dplyr::arrange(df_db, id), dplyr::arrange(df_ref, id))
})

test_that('Test appending partially overlapped table with missing value 2', 
{ 
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  my_fcn <- function(id, key = "id", ...) {
    set.seed(seed)
    data.frame("id" = id, "column_test" = rnorm(length(id)))
  }
  df_ref <- my_fcn(1:6)
  df_ref[5, 2] <- df_ref[6, 2] <- df_ref[1, 2]
  cached_fcn <- cache(my_fcn, prefix, salt, key = "id")
  cached_fcn(id = 1:5, key = "id", con = dbconn, 
    batch_data = batch_data, .select = "column_test")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  set_NULL(5)
  cached_fcn(id = 5:6, key = "id", con = dbconn,
    batch_data = batch_data, .select = "column_test")
  df_db <- db2df(dbReadTable(dbconn, dbListTables(dbconn)[2]), dbconn, "id")
  expect_equal(dplyr::arrange(df_db, id), dplyr::arrange(df_ref, id))
})
