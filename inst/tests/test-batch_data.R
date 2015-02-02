context('batch data')
library(DBI)

describe("batch data", {
  dbconn <- db_connection("database.yml", env = "avant")
  model_version <- "default/en-US/2.2.1"
  # key loan_ids does not match database column loan_id so a quick hack to do the testing
  batch_data <- function(loan_id, version, ...) avant::batch_data(loan_id, version, ...)
  test_that("it actually works for avant::batch_data", {
    expect_cached({
      df_ref <- batch_data(c(32835,32836), model_version)
      df_cached <- cached_fcn(c(32835,32836), model_version)
    })
  })

  dbDisconnect(dbconn)
})


