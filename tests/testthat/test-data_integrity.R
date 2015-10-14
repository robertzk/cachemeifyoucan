context('data integrity')

describe("data integrity", {

  test_that('it can expand a table if a new column pops up in later entries', {
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
    with_connection(dbconn(), {
    lapply(dbListTables(conn), function(t) dbRemoveTable(conn, t))
    cached_fcn <- cache(batch_data, key = c(key = "id"), c("model_version", "type"), con = conn, prefix = prefix, safe_columns = TRUE)
      cached_fcn(key = 5:1,  model_version, type)
      expect_error(cached_fcn(key = 1:10, model_version, type, add_column = TRUE))
    })
  })
})
