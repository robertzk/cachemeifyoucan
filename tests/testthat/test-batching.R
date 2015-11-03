context("batching while caching")
library(testthatsomemore)
library(dbtest)
library(DBI)
library(batchman)

db_test_that("it does not call batchman::batch when uncached keys do not exceed batch size", {
  env <- list2env(list(called = FALSE))
  package_stub("batchman", "batch", function(fn, ...) { env$called <- TRUE; fn }, {
    expect_cached({
      df_ref <- batch_data(1:100)
      df_cached <- cached_fcn(key = 1:100, model_version, type)
    })
    expect_false(env$called)
  })
})

db_test_that("it calls batchman::batch when uncached keys exceed batch size", {
  env <- list2env(list(called = FALSE))
  package_stub("batchman", "batch", function(fn, ...) { env$called <- TRUE; fn }, {
    expect_cached({
      df_ref <- batch_data(1:101)
      df_cached <- cached_fcn(key = 1:101, model_version, type)
    })
    expect_true(env$called)
  })
})
