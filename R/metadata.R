#' Create cache_meta table

CACHE_METADATA_TABLE <- "cache_metadata"

#' Track cache metadata and register salt if needed.
#'
#' @param dbconn SQLConnection. A database connection.
#' @param table_name character.  Name of cache table.
#' @param salt list. Actual value of salt before hashing.
track_cache_salt <- function(dbconn, table_name, salt) {
  ensure_cache_metadata_table_exists(dbconn)

  df <- DBI::dbGetQuery(dbconn, paste0("SELECT * from ", CACHE_METADATA_TABLE, " WHERE table_name = '", table_name, "'"))

  salt_obj <- serialize_to_string(salt)

  if (NROW(df) != 1) {
    if (!identical(salt_obj, df$salt_obj)) { browser(); stop("Cache salt values don't match what was registered.") }
  }

  df <- data.frame(table_name = table_name, salt_obj = salt_obj, stringsAsFactors = FALSE)
  dbWriteTableUntilSuccess(dbconn, CACHE_METADATA_TABLE, df, append = TRUE)
}

track_cache_salt_memoised <- memoise::memoise(track_cache_salt)


#' Get the salt used for a cache table
#'
#' @param dbconn SQLConnection. A database connection.
#' @param table_names character. The table names to inspect.
#' @export
get_cache_table_salt <- function(dbconn, table_names) {
  df <- get_cache_meta_data(dbconn, table_names)

  if (NROW(df) > 0) {
    warning("No matching entry found.")
    NULL
  } else {
    `names<-`(lapply(df$salt_obj, unserialize_from_string), df$table_name)
  }
}

get_cache_meta_data <- function(dbconn, table_names) {
  ensure_cache_metadata_table_exists(dbconn)

  query <- sprintf(
    "SELECT * FROM %s WHERE table_name IN (%s)",
    CACHE_METADATA_TABLE,
    paste(sprintf("'%s'", table_names), collapse = ", ")
  )

  df <- DBI::dbGetQuery(dbconn, query)
}

serialize_to_string <- function(obj) {
  rawToChar(serialize(obj, NULL, ascii = TRUE))
}

unserialize_from_string <- function(obj_str) {
  unserialize(charToRaw(obj_str))
}

ensure_cache_metadata_table_exists <- function(dbconn) {
  if (!DBI::dbExistsTable(dbconn, CACHE_METADATA_TABLE)) {
    DBI::dbGetQuery(dbconn, paste0("CREATE TABLE ", CACHE_METADATA_TABLE,
      " (table_name varchar(255) UNIQUE NOT NULL, salt_obj text NOT NULL)"))
  }
}
