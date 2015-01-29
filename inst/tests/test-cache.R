context('cachemeifyoucan:cache')
library(RPostgreSQL)
library(digest)

# Set up test fixture
# Set up local database for now
# https://github.com/hadley/dplyr/blob/master/vignettes/notes/postgres-setup.Rmd
prefix <- "version"
model_version <- "model_test"
dbconn <- dbConnect(dbDriver("PostgreSQL"), "feiye")

batch_data <- function(id, model_version = "model_test", type = "loan_id", 
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

test_that('Test inserting new table', 
{  
  # First remove all tables in the local database.
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  df_ref <- batch_data(1:5)
  cached_fcn <- cache(batch_data, prefix, key = "id", c("model_version", "type"), con = dbconn)
  df_cached <- cached_fcn(id = 1:5, "model_test", "loan_id")
  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, c(model_version, "loan_id"))), dbconn, "id")
  expect_equal(df_cached, df_ref)
  expect_equal(df_db, df_ref)
})

#test_that('Test appending partially overlapped table', 
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  df_ref <- batch_data(1:5)
#  cached_fcn <- cache(batch_data, prefix, salt, key = "id")
#  cached_fcn(id = 1:5, con = dbconn)
#  cached_fcn(id = 5, con = dbconn)
#  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "id")
#  expect_equal(df_db, df_ref)
#})
#
#test_that('Test appending fully overlapped table with missing value', 
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  df_ref <- batch_data(1:5, switch = TRUE, flip = 4:5)
#  cached_fcn <- cache(batch_data, prefix, salt, key = "id")
#  cached_fcn(id = 1:5, con = dbconn, switch = TRUE, flip = 5)
#  cached_fcn(id = 4, con = dbconn)
#  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "id")
#  expect_equal(dplyr::arrange(df_db, id), dplyr::arrange(df_ref, id))
#})
#
#test_that('Test appending partially overlapped table with missing value', 
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  df_ref <- batch_data(1:6, switch = TRUE, flip = c(1, 5, 6))
#  cached_fcn <- cache(batch_data, prefix, salt, key = "id")
#  cached_fcn(id = 1:5, key = "id", con = dbconn, switch = TRUE, flip = 1)
#  cached_fcn(id = 5:6, key = "id", con = dbconn)
#  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "id")
#  expect_equal(dplyr::arrange(df_db, id), dplyr::arrange(df_ref, id))
#})
#
#test_that('Test appending partially overlapped table with missing value 2', 
#{ 
#  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
#  df_ref <- batch_data(1:6, switch = TRUE, flip = c(1, 5, 6))
#  cached_fcn <- cache(batch_data, prefix, salt, key = "id")
#  cached_fcn(id = 1:5, key = "id", con = dbconn, 
#    switch = TRUE, flip = 1, .select = c("x", "y"))
#  cached_fcn(id = 5:6, key = "id", con = dbconn,
#    .select = c("x", "y"))
#  df_db <- db2df(dbReadTable(dbconn, table_name(prefix, salt)), dbconn, "id")
#  expect_equal(dplyr::arrange(df_db, id), dplyr::arrange(df_ref, id))
#})
