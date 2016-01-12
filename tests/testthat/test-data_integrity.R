context('data integrity')
library(dbtest)

describe("data integrity", {

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

  test_that('it crashes when trying to expand a table on new column when safe_columns is TRUE', {
    dbtest::with_test_db({
      lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
      cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = TRUE)
      cached_fcn(key = 5:1,  model_version, type)
      expect_error(cached_fcn(key = 1:10, model_version, type, add_column = TRUE))
    })
  })

  describe("when safe_column is a custom function", {
    test_that('it calls a custom function and returns without error', {
      dbtest::with_test_db({
        called <- FALSE
        caller <- function(...) { called <<- TRUE; message(as.list(...)); TRUE }
        lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
        cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = caller)
        cached_fcn(key = 5:1,  model_version, type)
        expect_false(called)
        cached_fcn(key = 1:10, model_version, type, add_column = TRUE)
        expect_true(called)
      })
    })

    test_that('it calls a custom function when safe_columns is is a function', {
      dbtest::with_test_db({
        called <- FALSE
        error_msg <- "Safe Columns Error: Customer function detected error."
        caller <- function(...) { called <<- TRUE; message(as.list(...)); stop(error_msg) }
        lapply(dbListTables(test_con), function(t) dbRemoveTable(test_con, t))
        cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = test_con, prefix = prefix, safe_columns = caller)
        cached_fcn(key = 5:1,  model_version, type)
        expect_false(called)
        expect_error(cached_fcn(key = 1:10, model_version, type, add_column = TRUE), error_msg)
        expect_true(called)
      })
    })
  })
})
