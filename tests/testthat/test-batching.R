context("batching while caching")
library(testthatsomemore)
library(DBI)
library(batchman)

test_that("it does not call batchman::batch when uncached keys do not exceed batch size", {
  env <- list2env(list(called = FALSE))
  package_stub("batchman", "batch", function(fn, ...) { env$called <- TRUE; fn }, {
    expect_cached({
      df_ref <- batch_data(1:100)
      df_cached <- cached_fcn(key = 1:100, model_version, type)
    })
    expect_false(env$called)
  })
})

test_that("it calls batchman::batch when uncached keys exceed batch size", {
  env <- list2env(list(called = FALSE))
  package_stub("batchman", "robust_batch", function(...) { env$called <- TRUE; batchman::get_before_fn(..1)(..2) }, {
    expect_cached({
      df_ref <- batch_data(1:101)
      df_cached <- cached_fcn(key = 1:101, model_version, type)
    })
    expect_true(env$called)
  })
})

