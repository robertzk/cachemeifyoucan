context('data integrity')

describe("safe columns", {
  db_test_that('it can expand a table if a new column pops up in later entries', {
    expect_cached({
      df_ref <- cbind(batch_data(1:5, model_version, type),
         data.frame(new_col = rep(NA_real_, 5)))
      df_ref <- rbind(df_ref, batch_data(6:10, model_version, type, add_column = TRUE))
      cached_fcn(key = 5:1,  model_version, type)
      df_cached <- without_rownames(cached_fcn(
        key = 1:10, model_version, type, add_column = TRUE))
      expect_almost_equal(without_rownames(df_ref),
        without_rownames(cached_fcn(key = 1:10, model_version, type)))
    }, no_check = TRUE)
  })

  describe("when safe_column is TRUE", {
     db_test_that('it crashes when trying to expand a table on new column', {
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = TRUE)
      cached_fcn(key = 5:1,  model_version, type)
      expect_error(cached_fcn(key = 1:10, model_version, type, add_column = TRUE))
    })
  })

  describe("when safe_column is a custom function", {
    db_test_that('it calls a custom function and returns without error', {
      called <- FALSE
       caller <- function(...) { called <<- TRUE; message(as.list(...)); TRUE }
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = caller)
      cached_fcn(key = 5:1,  model_version, type)
      expect_false(called)
      cached_fcn(key = 1:10, model_version, type, add_column = TRUE)
      expect_true(called)
    })

    db_test_that('it calls a custom function when safe_columns is is a function', {
      called <- FALSE
      error_msg <- "Safe Columns Error: Customer function detected error."
      caller <- function(...) { called <<- TRUE; message(as.list(...)); stop(error_msg) }
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = caller)
      cached_fcn(key = 5:1,  model_version, type)
      expect_false(called)
      expect_error(cached_fcn(key = 1:10, model_version, type, add_column = TRUE), error_msg)
      expect_true(called)
    })
  })
})


describe("conditional caching", {
  db_test_that("it will cache NA if NA is not on the blacklist", {
    expect_cached({
     cached_fcn <- cache(return_nas, key = c(key = "id"), salt = c("model_version", "type"), con = test_con, prefix = prefix, blacklist = list("pizza"))
     df_ref <- return_nas(1:5)
     expect_almost_equal(without_rownames(df_ref),
       without_rownames(cached_fcn(key = 1:5, model_version, type)))
    }, fn = return_nas)
  })

  db_test_that("it won't cache NA if NA is on the blacklist", {
    expect_cached({
      lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
      cached_fcn <- cache(return_nas, key = c(key = "id"), salt = c("model_version", "type"), con = test_con, prefix = prefix, blacklist = list(NA))
       # Will cache to a strange 0x2 df...
      df_ref <- data.frame(id = 1, data = "a", stringsAsFactors = FALSE)[FALSE, ]
      expect_almost_equal(without_rownames(return_nas(1:5)),  # ...But will return the normal data
        without_rownames(cached_fcn(key = 1:5, model_version, type)))
    }, fn = return_nas, no_check = TRUE)
  })

  db_test_that("it won't cache FALSE if FALSE is on the blacklist", {
    expect_cached({
      lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
      cached_fcn <- cache(return_falses, key = c(key = "id"), salt = c("model_version", "type"), con = test_con, prefix = prefix, blacklist = list("FALSE"))
      # Will cache to a strange 0x2 df...
      df_ref <- data.frame(id = 1, data = "a", stringsAsFactors = FALSE)[FALSE, ]   
      expect_almost_equal(without_rownames(return_falses(1:5)),  # ...But will return the normal data
        without_rownames(cached_fcn(key = 1:5, model_version, type)))
    }, fn = return_falses, no_check = TRUE)
  })

  db_test_that("it won't cache 'pizza' or 'potato' if they're on the blacklist", {
    expect_cached({
      lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
      cached_fcn <- cache(return_foods, key = c(key = "id"), salt = c("model_version", "type"), con = test_con, prefix = prefix, blacklist = list("pizza", "potato"))
      df_ref <- data.frame(id = c(3, 4), data = c("apple", "banana"), stringsAsFactors = FALSE)
      expect_almost_equal(without_rownames(return_foods(seq(5))),
        without_rownames(cached_fcn(key = 1:5, model_version, type)))
    }, fn = return_foods, no_check = TRUE)
  })

  db_test_that("it will cache what is not on the blacklist", {
    expect_cached({
      cached_fcn <- cache(return_mix_na, key = c(key = "id"),
                          salt = c("model_version", "type"),
                          con = test_con, prefix = prefix, blacklist = list(NA))
      df_ref <- data.frame(id = c(2L, 4L), data = rep("value", 2),
        stringsAsFactors = FALSE)
      df_actual <- data.frame(id = seq(5), data = ifelse(seq(5) %% 2 == 0, "value", NA),
        stringsAsFactors = FALSE)
      expect_equal(without_rownames(df_actual),
        without_rownames(cached_fcn(key = seq(5), model_version, type)))
    }, fn = return_mix_na)
  })

  db_test_that("it will cache what is not on the blacklist", {
    expect_cached({
      cached_fcn <- cache(return_mix_false, key = c(key = "id"), 
                          salt = c("model_version", "type"),
                          con = test_con, prefix = prefix, blacklist = list(FALSE))
      df_ref <- data.frame(id = c(2L, 4L), data = rep("value", 2),
        stringsAsFactors = FALSE)
      df_actual <- data.frame(id = seq(5), data = ifelse(seq(5) %% 2 == 0, "value", FALSE),
        stringsAsFactors = FALSE)
      expect_equal(without_rownames(df_actual),
        without_rownames(cached_fcn(key = seq(5), model_version, type)))
    }, fn = return_mix_false)
  })
})
