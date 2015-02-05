#' Remove the database caching layer to an arbitrary R function.
#'
#' @param uncached_function function. The function to cache.
#' @param key character. A character vector of primary keys. The user
#'   guarantees that \code{uncached_function} has these as formal arguments
#'   and that it returns a data.frame containing columns with at least those
#'   names. For example, if we are caching a function that looks like
#'   \code{function(author) { ... }}, we expect its output to be data.frames
#'   containing an \code{"author"} column with one record for each author.
#'   In this situation, \code{key = "author"}.
#' @param salt character. The names of the formal arguments of \code{uncached_function}
#'   for which a unique value at calltime should use a different database
#'   table. In other words, if \code{uncached_function} has arguments 
#'   \code{id, x, y}, but different kinds of data.frames (i.e., ones with
#'   different types and/or column names) will be returned depending
#'   on the value of \code{x} or \code{y}, then we can set
#'   \code{salt = c("x", "y")} to use a different database table for
#'   each combination of values of \code{x} and \code{y}. For example,
#'   if \code{x} and \code{y} are only allowed to be \code{TRUE} or
#'   \code{FALSE}, with potentially four different kinds of data.frame
#'   outputs, then up to four tables would be created.
#' @param con SQLConnection. Database connection object.
#' @param prefix character. Database table prefix. A different prefix should
#'   be used for each cached function so that there are no table collisions.
#'   Optional, but highly recommended.
#' @param env character. The environment of the database connection if con 
#'   is a yaml cofiguration file.
#' @return A function that removes underlying database table.
#' @export
uncache <- function(uncached_function, key, salt, con, prefix, env) {
  stopifnot(is.function(uncached_function),
    is.character(prefix), length(prefix) == 1,
    is.character(key), length(key) > 0,
    is.atomic(salt) || is.list(salt))

  con <- build_connection(con, env)

  cached_function <- new("function")

  # Retain the same formal arguments as the base function.
  formals(cached_function) <- formals(uncached_function)

  # Inject some values we will need in the body of the caching layer.
  environment(cached_function) <- 
    list2env(list(prefix = prefix, key = key, salt = salt,
      uncached_function = uncached_function, con = con, force = force),
      parent = environment(uncached_function))

  build_uncached_function(cached_function)
}

build_uncached_function <- function(cached_function) {
  # All cached functions will have the same body.
  body(cached_function) <- quote({
    # If a user calls the uncached_function with, e.g.,
    #   fn <- function(x, y) { ... }
    #   fn(1:2), fn(x = 1:2), fn(y = 5, 1:2), fn(y = 5, x = 1:2)
    # then `call` will be a list with names "x" and "y" in all
    # situations.
    raw_call <- match.call()
    call     <- as.list(raw_call[-1]) # Strip function name but retain arguments.
    
    # Evaluate function call parameters in the calling environment
    for (name in names(call))
      call[[name]] <- eval.parent(call[[name]])

    # Only apply salt on provided values.
    true_salt <- call[intersect(names(call), salt)]
    
    # Since the values in `call` might be expressions, evaluate them
    # in the calling environment to get their actual values.
    for (name in names(true_salt)) {
      true_salt[[name]] <- eval.parent(true_salt[[name]])
    }

    # The database table to use is determined by the prefix and
    # what values of the salted parameters were used at calltime.
    tbl_name <- cachemeifyoucan:::table_name(prefix, true_salt)
    DBI::dbRemoveTable(con, tbl_name)
  })

  cached_function
}
