#' Closure of a cache layer
#'
#' @param uncached_function function. The function to cache.
#' @param prefix character. Database table prefix.
#' @param key character. Key for the database backend.
#' @param salt atomic character vector. Database table salt.
#' @param con SQLConnection. Database connection.
#' @return a function with a cache layer of database backend.
#' @importFrom digest digest
#' @export
cache <- function(uncached_function, prefix, key, salt, con) {
  stopifnot(is.function(uncached_function),
    is.character(prefix),
    is.character(key),
    is.atomic(salt) || is.list(salt))

  cached_function <- new("function")

  formals(cached_function)     <- formals(uncached_function)

  environment(cached_function) <- 
    list2env(list(prefix = prefix, key = key, salt = salt,
      uncached_function = uncached_function, con = con),
      parent = environment(uncached_function))

  body(cached_function) <- quote({
    raw_call <- match.call()
    call <- as.list(raw_call[-1])
    true_salt <- call[intersect(names(call), salt)]
    for (name in names(true_salt)) 
      true_salt[[name]] <- eval.parent(true_salt[[name]])
    tbl_name <- table_name(prefix, true_salt)
    execute(
      cached_function_call(uncached_function, call, parent.frame(), tbl_name, key, con)
    )
  })

  cached_function
}

#' A helper function to execute the cached function call
execute <- function(fcn_call) {
  keys <- eval(fcn_call$call[[fcn_call$key]], envir = fcn_call$context)
  # Check which parts of key are already in fcn_call$table
  # Grab the new/old ids (might be integer(0))
  uncached_keys <- get_new_key(fcn_call$con, fcn_call$table, keys, fcn_call$key)
  cached_keys <- setdiff(keys, uncached_keys)
  # Work on the uncached data
  uncached_data <- data_injector(fcn_call, uncached_keys, FALSE)
  # Error check on the uncached data computed from the user-provided function
  uncached_data <- error_fn(uncached_data)
  # Grab the cached data (empty data frame when length 0)
  cached_data <- data_injector(fcn_call, cached_keys, TRUE)
  # Check on the cached data, recompute ids if missing on a given row
  missing_keys <- sapply(cached_keys, 
    function(x) switch(2 - any(is.na(cached_data[x, 
    switch(1 + (ncol(uncached_data) > 0), TRUE, 
      setdiff(colnames(uncached_data), fcn_call$key))])), x, NA))
  missing_keys <- missing_keys[!is.na(missing_keys)]
  cached_data <- data.frame()
  if (length(missing_keys) > 0) {
    #cat(paste0("These have missing columns: ", paste(missing_keys, collapse = " "), "\n"))
    fcn_call$call[[fcn_call$key]] <- missing_keys
    cached_data <- eval(as.call(append(fcn_call$fn, fcn_call$call)),
      envir = fcn_call$context)
    # Fetch cached data from db
    cached_data_db <- read_data(fcn_call$con, fcn_call$table, 
      cached_data[[fcn_call$key]], fcn_call$key)
    # Merge on-the-fly cached data with db
    # Stop if something happens concurrently, 
    # pretending the following line of code are atomically executed...
    stopifnot(setequal(cached_data_db[[fcn_call$key]], cached_data[[fcn_call$key]]), 
        remove_rows(fcn_call$con, fcn_call$table, 
          cached_data[[fcn_call$key]], fcn_call$key))
    cached_data_db <- cached_data_db[, !colnames(cached_data_db) %in% 
      setdiff(colnames(cached_data), fcn_call$key), drop = FALSE]
    cached_data <- merge(cached_data, cached_data_db, by = fcn_call$key)
  }
  cached_data <- error_fn(cached_data)
  # Combine uncached and cached data
  df_combine <- plyr::rbind.fill(uncached_data, cached_data)
  # Cache them
  write_data_safely(fcn_call$con, fcn_call$table, df_combine, fcn_call$key)
  df_combine
}
