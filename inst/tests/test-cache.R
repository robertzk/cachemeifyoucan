context('cache')
library(RPostgreSQL)
library(testthatsomemore)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
salt <- "model_test"
dbconn <- dbConnect(dbDriver("PostgreSQL"), "feiye")

my_batch_data <- function(loan_ids, version = "model_test", ..., 
  verbose = TRUE, strict = TRUE, cache = TRUE, .depth = 0) {
  dbReadTable(dbconn, table_name(version))
}

test_that('Test insert new table', 
package_stub("avant", "batch_data", my_batch_data,
{ 
  my_fcn <- function(loan_id, key = "loan_id") {
   data.frame("loan_id" = loan_id, "column_test" = rnorm(length(loan_id)))
  }
  print(my_fcn(1:5))
  cached_fcn <- cache(my_fcn, prefix, salt, dbconn, "loan_id")
  cached_fcn(loan_id = 1:5, key = "loan_id")
  print(dbReadTable(con, dbListTables(con)[2]))
  assert(TRUE) 
}))
