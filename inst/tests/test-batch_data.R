context('batch data')
library(DBI)

dbconn <- db_connection("database.yml", env = "avant")
prefix <- "version"
model_version <- "default/en-US/2.2.1"
type <- "loan_id"
# key loan_ids does not match database column loan_id so a quick hack to do the testing
batch_data <- function(loan_id, version, ...) avant::batch_data(loan_id, version, ...)

describe("batch data", {
  test_that("it actually works for avant::batch_data", {
    expect_cached_batch_data({
      df_ref <- batch_data(c(32835,32836), model_version)
      df_cached <- cached_fcn(c(32835,32836), model_version)
      no_check <- TRUE
    })
  })

  dbDisconnect(dbconn)
})
