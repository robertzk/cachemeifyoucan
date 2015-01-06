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

  get_new_key <- function(dbconn, salt, ids, table_name) {
    if (length(ids) == 0) return(NULL)
    id_column_name <- get_hashed_names(key)
    present_ids <- dbGetQuery(dbconn, paste0(
      "SELECT ", id_column_name, " FROM ", table_name))[[1]]
    setdiff(ids, present_ids)
  }

  tbl_name <- paste0(prefix, digest(salt))
  dbconn <- db_for_syberia_project(root)

  function(...) {
    # Check if user-provided function key matches the closure. 
    if (is.null(tryCatch(match.arg(key, names(formals(fn))), error = function(...) NULL)))
      stop("Keys must match", call. = FALSE)
    # Go get the function arguments
    args <- eval.parent(substitute(alist(...)))
    if (!key %in% names(args)) 
      stop("Key argument must be named", call. = FALSE)
    # Grab the new ids
    ids_new <- get_new_key(dbconn, salt, args[[key]], tbl_name)
    ids_old <- setdiff(args[[key]], ids_new)
    if (length(ids_new) == 0)
      print("Nothing new to add on")
    data_old <- batch_data(ids_old, salt, strict = TRUE, cache = FALSE)
    # Work on the new data
    args[[key]] <- ids_new
    data_new <- do.call(fn, args)
    if (!is.data.frame(data_new))
      stop("User-provided function must return a data frame", call. = FALSE)
    if (any(is.na(data_new)))
      stop("NA is returned in user-provided function", call. = FALSE)
    # Check on the old data, recompute if missing on a given row
    colnames_check <- setdiff(colnames(data_new), key)
    ids_missing <- sapply(ids_old, 
                          function(x) switch(any(is.na(data_old[x, colnames_check])), NULL, x))
    data_old <- NULL
    if (length(ids_missing) > 0) {
      args[[key]] <- ids_missing
      data_old <- do.call(fn, args)
    }
    # Combine new data and old data
    df <- rbind(data_new, data_old)
    # Cache them
    write_data_safely(dbconn, tbl_name, df)
  }
}
