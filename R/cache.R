#' Closure of a cache layer
#'
#' @param fn. The function to cache.
#' @param prefix. Database table prefix.
#' @param salt. Database table salt.
#' @param key. Keys for the database backend.
#i @param root. Syberia root.
#i @import avant syberiaStructure
#' @return a function with a cache layer of database backend.
#' @export
cache <- function(fn, prefix, salt, key = "loan_id", root = syberia_root()) {
  stopifnot(is.function(fn))
  stopifnot(is.character(prefix))
  stopifnot(is.character(salt))
  stopifnot(is.character(key))

  get_new_loan_data <- function(dbconn, version, loans, table_name) {
#    if (length(loans) == 0) return(NULL)
#    # Fetch loan_ids already present.
#    loan_id_column_name <- get_hashed_names(key)
#    present_loan_ids <- dbGetQuery(dbconn, paste0(
#      "SELECT ", loan_id_column_name, " FROM ", table_name))[[1]]
#    loans <- setdiff(loans, present_loan_ids)
#
#    # loans now contains the loan ids not yet present in the database.
#    if (length(loans) > 0) batch_data(loans, version, strict = TRUE, cache = FALSE)
#    else NULL
    data.frame(key = 1, value = 2)
  }

  tbl_name <- paste0(prefix, digest(salt))
  dbconn <- db_for_syberia_project(root)

  function(...) {
    # Check if user-provided function key matches the closure. 
    if (is.null(tryCatch(match.arg(key, names(formals(fn))), error = function(...) NULL)))
      stop("Keys must match", call. = FALSE)
    # Go get the function arguments
    args <- list(...)
    names(args) <- names(formals(fn))
    # Grab the new data.
    data_new <- get_new_loan_data(dbconn, salt, args[[key]], tbl_name)
    # Work on the new data.
    # Check on the old data.
    # Combine new data and old data.
    # Cache them.
    fn(...)
  }
}
