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
cache <- function(fn, prefix, salt, dbconn, key = "loan_id", root = syberia_root()) {
  stopifnot(is.function(fn))
  stopifnot(is.character(prefix))
  stopifnot(is.character(salt))
  stopifnot(is.character(key))

  get_new_key <- function(dbconn, salt, ids, table_name) {
    if (length(ids) == 0) return(integer(0))
    if (!dbExistsTable(dbconn, tbl_name)) return(ids)
    id_column_name <- get_hashed_names(key)
    present_ids <- dbGetQuery(dbconn, paste0(
      "SELECT ", id_column_name, " FROM ", table_name))[[1]]
    setdiff(ids, present_ids)
  }

  error_fn <- function(data) {
    if (!is.data.frame(data))
      stop("User-provided function must return a data frame", call. = FALSE)
    if (any(is.na(data)))
      stop("NA is returned in user-provided function", call. = FALSE)
  }

  tbl_name <- paste0(prefix, "_", digest(salt))
  if (missing(dbconn)) dbconn <- db_for_syberia_project(root)

  function(...) {
    # Check if user-provided function key matches the closure. 
    if (is.null(tryCatch(identical(key, formals(fn)$key), error = function(...) NULL)))
      stop("Keys must match", call. = FALSE)
    # Go get the function arguments
    args <- eval.parent(substitute(alist(...)))
    args <- sapply(names(args), function(n) eval(args[[n]]), simplify = FALSE, USE.NAMES = TRUE)
    if (!"key" %in% names(args)) 
      stop("Key argument must be named", call. = FALSE)
    # Grab the new/old ids (might be integer(0))
    ids_new <- get_new_key(dbconn, salt, args[[key]], tbl_name)
    ids_old <- setdiff(c(args[[key]]), ids_new)
    # Work on the new data
    if (length(ids_new) == 0) {
      #cat(paste0("All data is cached, unless they are missing", "\n"))
      data_new <- data.frame()
    } else {
      args[[key]] <- ids_new
      data_new <- do.call(fn, args)
    }
    # Error check on the new data computed from the user-provided function
    error_fn(data_new)
    # Grab the old data (empty data frame when length 0)
    if (length(ids_old) == 0) {
      #cat(paste0("No data is cached", "\n"))
      data_old <- data.frame()
    } else {
      #cat(paste0("These are cached: ", paste(ids_old, collapse = " "), "\n"))
      if (".select" %in% names(args) && dbExistsTable(dbconn, tbl_name)) 
        data_old <- db2df(dbGetQuery(dbconn,
          paste("SELECT ", 
                paste(get_hashed_names(c(args$.select, key)), collapse = ","), 
                " FROM", tbl_name, "WHERE ", key, " IN (",
          paste(ids_old, collapse = ', '), ")")), dbconn)
      else 
        data_old <- batch_data(ids_old, salt, strict = TRUE, cache = FALSE)
    }
    # Check on the old data, recompute ids if missing on a given row
    if (ncol(data_new) > 0)
      colnames_check <- setdiff(colnames(data_new), key)
    else if (".select" %in% names(args))
      colnames_check <- args$.select
    if (!exists("colnames_check", inherits = FALSE)) {
      ids_missing <- sapply(ids_old, 
        function(x) switch(2 - any(is.na(data_old[x, ])), x, NA))
      ids_missing <- ids_missing[!is.na(ids_missing)]
    } else {
      ids_missing <- sapply(ids_old, 
        function(x) switch(2 - any(is.na(data_old[x, colnames_check])), x, NA))
      ids_missing <- ids_missing[!is.na(ids_missing)]
    }
    data_old <- data.frame()
    if (length(ids_missing) > 0) {
      #cat(paste0("These have missing columns: ", paste(ids_missing, collapse = " "), "\n"))
      args[[key]] <- ids_missing
      data_old <- do.call(fn, args)
      # Fetch old data from db
      data_old_db <- read_data(dbconn, tbl_name, data_old[[key]], key)
      # Merge on-the-fly data_old with db
      # Stop if something happens concurrently, 
      # pretending the following line of code are atomically executed...
      stopifnot(setequal(data_old_db[[key]], data_old[[key]]), 
          remove_rows(dbconn, tbl_name, data_old[[key]], key))
      data_old_db <- data_old_db[, !colnames(data_old_db) %in% setdiff(colnames(data_old), key), 
        drop = FALSE]
      data_old <- merge(data_old, data_old_db, by = key)
    }
    error_fn(data_old)
    # Combine new data and old data
    df_combine <- plyr::rbind.fill(data_new, data_old)
    # Cache them
    write_data_safely(dbconn, tbl_name, df_combine)
    df_combine
  }
}
