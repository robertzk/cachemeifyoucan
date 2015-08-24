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
  package_stub("batchman", "batch", function(fn, ...) { env$called <- TRUE; fn }, {
    expect_cached({
      df_ref <- batch_data(1:101)
      df_cached <- cached_fcn(key = 1:101, model_version, type)
    })
    expect_true(env$called)
  })
})

test_that("it can call batchman::batch with two keys when needed (issue #72)", {
  env <- list2env(list(called = FALSE))
  package_stub("batchman", "batch", function(...) { browser(); env$called <- TRUE; fn }, {
    two_key <- function(alpha_key, beta_key) {
      mapply(sum, alpha_key, beta_key)
    }
    df_ref <- two_key(1:101, 1:101)
    cached_two_key <- cache(two_key, key = c(key = c("alpha_key", "beta_key")), character(0))
    df_cached <- cached_two_key(alpha_key = 1:101, beta_key = 1:101)
  })
  expect_true(env$called)
})
