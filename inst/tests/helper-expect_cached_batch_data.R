expect_cached_batch_data <- function(expr) {
  dbconn <- get("dbconn", envir = parent.frame())
  prefix <- get("prefix", envir = parent.frame())
  model_version <- get("model_version", envir = parent.frame())
  type <- get("type", envir = parent.frame())
  batch_data <- get("batch_data", envir = parent.frame())
  lapply(dbListTables(dbconn), function(t) dbRemoveTable(dbconn, t))
  cached_fcn <- cache(batch_data, key = "loan_id", "version", con = dbconn, prefix = prefix)
  eval(substitute(expr), envir = environment())
  df_db <- db2df(dbReadTable(dbconn, cachemeifyoucan:::table_name(prefix, list(version = model_version))), dbconn, "loan_id")
  # Clumsy to check matching database
  if (!exists('no_check', envir = environment(), inherits = FALSE) ) {
    df_db <- df_db[match(df_ref$loan_id, df_db$loan_id), ]
    equalN <- sum(vapply(colnames(df_ref), 
      function(nn) if (is.logical(df_ref[[nn]]) || 
        is.character(df_ref[[nn]]) || is.factor(df_ref[[nn]])) 
        all.equal(as.character(df_ref[[nn]]), df_db[[nn]]) 
      else 
        all.equal(df_ref[[nn]], df_db[[nn]]), logical(1))) 
    expect_equal(equalN, ncol(df_ref))
  }
  if (exists('df_cached', envir = environment(), inherits = FALSE)) {
    equalN <- sum(vapply(colnames(df_ref), 
      function(nn) if (is.logical(df_ref[[nn]]) || 
        is.character(df_ref[[nn]]) || is.factor(df_ref[[nn]])) 
        all.equal(as.character(df_ref[[nn]]), df_cached[[nn]]) 
      else 
        all.equal(df_ref[[nn]], df_cached[[nn]]), logical(1))) 
    expect_equal(equalN, ncol(df_ref))
  }
}
