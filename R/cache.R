#' Apply a database caching layer to an arbitrary R function.
#'
#' Requesting data from an API or performing queries can sometimes yield
#' dataframe outputs that are easily cached according to some primary key.
#' This function makes it possible to cache the output along the primary
#' key, while only using the uncached function on those records that 
#' have not been computed before.
#'
#' @param uncached_function function. The function to cache.
#' @param prefix character. Database table prefix.
#' @param key character. Key for the database backend.
#' @param salt atomic character vector. Database table salt.
#' @param con SQLConnection. Database connection.
#' @return a function with a cache layer of database backend.
#' @export
#' @examples
#' \dontrun{
#' # These examples assume you have a database connection object
#' # (as specified in the DBI package) in a local variable `con`.
#' 
#' # Imagine we have a function that returns a data.frame of information
#' # about IMDB titles through their API. It takes an integer vector of
#' # IDs and returns a data.frame with an "id" column, with one row for
#' # each title. (for example, 111161 would correspond to 
#' # http://www.imdb.com/title/tt111161/ which is The Shawshank Redemption).
#' amazon_info <- function(id) {
#'   # Call external API.
#' }
#'
#' # Sending HTTP requests to Amazon and waiting for the response is
#' # computationally intensive, so if we ask for some IDs that have
#' # already been computed in the past, it would be useful to not 
#' # make additional HTTP requests for those records. For example,
#' # we may want to do some processing on all Amazon titles. However,
#' # new records are created each day. Instead of parsing all
#' # the historical records on each execution, we would like to only
#' # parse new records; old records would be retrieved from a database
#' # table that had the same column names as a typical output data.frame
#' # of the `amazon_info` function.
#' cached_amazon_info <- cachemeifyoucan::cache(amazon_info, key = 'id', con = con)
#'
#' # By using the `cache` function, we are asking for the following:
#' #   (1) If we call `cached_amazon_info` with a vector of integer IDs,
#' #       take the subset of IDs that have already been returned from
#' #       a previous call to `cached_amazon_info`. Retrieve the data.frame
#' #       for these records from an underlying database table.
#' #   (2) The remaining IDs (those we have never passed to `cached_amazon_info`
#' #       should be fed to the base `amazon_info` function as if we had
#' #       called it with this subset. This will yield another data.frame that
#' #       was computed using live HTTP requests.
#' # The `cached_amazon_info` function will return the union (rbind) of these
#' # two data sets as one single data set, as if we had called `amazon_info`
#' # by itself. It will also cache the second data set so another identical
#' # call to `cached_amazon_info` will not trigger any additional HTTP requests.
#'
#' ###
#' # Salts
#' ###
#' 
#' }
cache <- function(uncached_function, prefix, key, salt, con) {
  stopifnot(is.function(uncached_function),
    is.character(prefix),
    is.character(key),
    is.atomic(salt) || is.list(salt))

  cached_function <- new("function")

  formals(cached_function) <- formals(uncached_function)

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
    tbl_name <- cachemeifyoucan:::table_name(prefix, true_salt)
    execute(
      cached_function_call(uncached_function, call, parent.frame(), tbl_name, key, con)
    )
  })

  cached_function
}

#' A helper function to execute the cached function call
execute <- function(fcn_call) {
  keys <- eval(fcn_call$call[[fcn_call$key]], envir = fcn_call$context)
  # Grab the new/old keys
  uncached_keys <- get_new_key(fcn_call$con, fcn_call$table, keys, fcn_call$key)
  cached_keys <- setdiff(keys, uncached_keys)
  # Work on the uncached data
  uncached_data <- data_injector(fcn_call, uncached_keys, FALSE)
  # Error check on the uncached data computed from the user-provided function
  uncached_data <- error_fn(uncached_data)
  # Grab the cached data
  cached_data <- data_injector(fcn_call, cached_keys, TRUE, FALSE)
  # Check on the cached data, recompute ids if missing on a given row
  missing_keys <- sapply(cached_keys, 
    function(x) switch(2 - any(is.na(cached_data[x, 
    switch(1 + (ncol(uncached_data) > 0), TRUE, 
      setdiff(colnames(uncached_data), fcn_call$key))])), x, NA))
  missing_keys <- missing_keys[!is.na(missing_keys)]
  cached_data <- data_injector(fcn_call, missing_keys, TRUE, TRUE)
  cached_data <- error_fn(cached_data)
  # Combine uncached and cached data
  df_combine <- plyr::rbind.fill(uncached_data, cached_data)
  # Cache them
  write_data_safely(fcn_call$con, fcn_call$table, df_combine, fcn_call$key)
  df_combine
}
