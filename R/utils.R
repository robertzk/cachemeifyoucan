`%||%` <- function(x, y) if (is.null(x)) y else x

slice <- function(x, n) split(x, as.integer((seq_along(x) - 1) / n))

#' Sync credit model data between Rails and R
#'
#' @param version character. The version of the model whose
#'   data we would like to sync.
#' @param loans numeric. A list of reference load ids by which
#'   to sync the dataset.
#' @param root character. Root of the relevant syberia project.
#'   The default is \code{syberia_root()}. This project must define
#'   a \code{db} configuration option in its syberia config file.
#'   This database will be used for syncing.
#' @importFrom digest digest
#' @return \code{TRUE} or \code{FALSE} according as the sync is 
#'   successful or insuccessful.
#' @export
data_sync <- function(version, loans, root = syberia_root()) {
  establish_database_connection <- function(root, version, loans, table_name) {
    dbconn <- db_for_syberia_project(root)
    # Make the table for this model version if it does not exist
    if (!dbExistsTable(dbconn, table_name)) {
      sample_data <- batch_data(loans, version, cache = FALSE)
      # TODO: (RK) What if some columns are missing?
      write_data_safely(dbconn, table_name, sample_data)
    }
    dbconn
  }

  get_new_loan_data <- function(dbconn, version, loans, table_name) {
    if (length(loans) == 0) return(NULL)
    # Fetch loan_ids already present.
    loan_id_column_name <- get_hashed_names('loan_id')
    present_loan_ids <- dbGetQuery(dbconn, paste0(
      "SELECT ", loan_id_column_name, " FROM ", table_name))[[1]]
    loans <- setdiff(loans, present_loan_ids)

    # loans now contains the loan ids not yet present in the database.
    if (length(loans) > 0) batch_data(loans, version, strict = TRUE, cache = FALSE)
    else NULL
  }

  stopifnot(is.character(version))
  stopifnot(is.numeric(loans) && length(loans) > 0)
  tbl_name <- table_name(version)
  dbconn <- establish_database_connection(root, version, loans, tbl_name)
  new_data <- get_new_loan_data(dbconn, version, loans, tbl_name)
  write_data_safely(dbconn, tbl_name, new_data)
}

#' Fetch table name that caches data for a model version.
#' 
#' @param version character. Model version.
#' @name table_name
#' @importFrom digest digest
#' @return the table name. This will just be \code{"version_"}
#'   appended with the MD5 digest of the model version.
table_name <- function(version) {
  paste0("version_", digest(version))
}

#' Fetch cached loan data from local storage.
#'
#' @param loans numeric. A vector of loan IDs.
#' @param version character. The model version for which to fetch data.
#' @param root character. Current syberia root project. Default is
#'   \code{syberia_root()}.
#' @return a data.frame of loans that could be fetched from the cache.
#' @export
cached_data <- function(loans, version, root = syberia_root()) {
  stopifnot(is.numeric(loans))
  stopifnot(is.character(version))
  dbconn <- db_for_syberia_project(root)
  tbl_name <- table_name(version)
  if (length(loans) == 0 || !isTRUE(dbExistsTable(dbconn, tbl_name))) return(data.frame())
  loan_id_column_name <- get_hashed_names('loan_id')
  df <- dbGetQuery(dbconn,
    paste("SELECT * FROM", tbl_name, "WHERE ", loan_id_column_name, " IN (",
      paste(loans, collapse = ', '), ")"))
  # Remove the non-hashed columns (like ID columns)
  non_hashed_cols <- !grepl('^c[0-9a-f]{32}$', colnames(df))
  df[, non_hashed_cols] <- vector('list', sum(non_hashed_cols))
  colnames(df) <- translate_column_names(colnames(df), dbconn)
  df <- df[!duplicated(df), ]
  df
}

#' Save loan data into a database cache.
#'
#' @param version character. Model version.
#' @param uncached_data dataframe. The data to save.
save_loan_data_in_cache <- function(version, uncached_data) {
  tblname <- table_name(version)
  write_data_safely(db_for_syberia_project(), tblname, uncached_data)
}

#' Fetch a database connection for a syberia project given
#' its root.
#' 
#' @param root character. The root of the Syberia project.
#'   The default is \code{syberia_root()}.
#' @name db_for_syberia_project
#' @importFrom testthat colourise
#' @return a database connection object.
db_for_syberia_project <- local({
  conn <- NULL
  function(root = syberia_root()) {
    if (!is.null(conn)) return(conn)
    require(yaml)
    database.yml <- file.path(root, 'config', 'database.yml')
    database.yml <- paste(readLines(database.yml), collapse = "\n")
    config.database <- yaml.load(database.yml)
    stopifnot('avant' %in% names(config.database))
    config.database <- config.database$avant
    # TODO: (RK) Support different adapters.
    config.database$adapter <- config.database$adapter %||% 'postgresql'
    config.database$host <- config.database$host %||% '127.0.0.1'
    config.database$port <- as.integer(config.database$port %||% 5432)

    require(RPostgreSQL)
    conn <<- dbConnect(dbDriver("PostgreSQL"), dbname = config.database$database,
              user = config.database$username, password = config.database$password,
              port = config.database$port, host = config.database$host)
  }
})

#' MD5 digest of column names.
#' @param raw_names character. A character vector of column names.
#' @name get_hashed_names
#' @return the character vector of hashed names.
get_hashed_names <- function(raw_names) {
  require(digest)
  paste0('c', sapply(raw_names, digest::digest))
}

#' Fetch the map of column names.
#' @name column_names_map
#' @param dbconn SQLConnection. A database connection.
column_names_map <- function(dbconn) {
  dbGetQuery(dbconn, 'SELECT * FROM column_names')
}

#' Translate column names using the column_names table from MD5 to raw.
#' @name translate_column_names
#' @param names character. A character vector of column names.
#' @param dbconn SQLConnection. A database connection.
translate_column_names <- function(names, dbconn) {
  name_map <- column_names_map(dbconn)
  name_map <- setNames(as.list(name_map$raw_name), name_map$hashed_name)
  sapply(names, function(name) name_map[[name]] %||% name)
}

#' Helper utility for safe IO of a data.frame to a database connection.
#'
#' This function will be mindful of two problems: non-existent columns
#' and long column names.
#'
#' Since this is meant to be used as a helper function for caching loan
#' data, we must take a few precautions. If certain variables are not
#' available for older data but are introduced for newer data, we
#' must be careful to create those columns first.
#'
#' Furthermore, certain column names may be longer than PostgreSQL supports.
#' To circumvent this problem, this helper function stores an MD5
#' digest of each column name and maps them using the `column_names`
#' helper table.
#'
#' By default, this function assumes any data to be written is not
#' already present in the table and should be appended. If the table does
#' not exist, it will be created.
#'
#' @name write_data_safely
#' @param dbconn PostgreSQLConnection. The database connection.
#' @param tblname character. The table name to write the data into.
#' @param df data.frame. The data to write.
write_data_safely <- function(dbconn, tblname, df) {
  if (is.null(df)) return(FALSE)
  if (!is.data.frame(df)) return(FALSE)
  if (nrow(df) == 0) return(FALSE)

  id_cols <- grep('(_|^)id$', colnames(df), value = TRUE)
  if (length(id_cols) == 0)
    stop("The data you are writing to the database must contain at least one ",
         "column ending with '_id'")

  write_column_names_map <- function(raw_names) {
    hashed_names <- get_hashed_names(raw_names)
    column_map <- data.frame(raw_name = raw_names, hashed_name = hashed_names)
    column_map <- column_map[!duplicated(column_map), ]

    # If we don't do this, we will get really weird bugs with numeric things stored as character
    # For example, a loan with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20) 
    on.exit(options(old_options))

    # Store the map of raw to MD5'ed column names in the column_names table.
    if (!dbExistsTable(dbconn, 'column_names'))
      dbWriteTable(dbconn, 'column_names', column_map, row.names = 0)
    else {
      raw_names <- dbGetQuery(dbconn, 'SELECT raw_name FROM column_names')[[1]]
      column_map <- column_map[!is.element(column_map$raw_name, raw_names), ]
      if (NROW(column_map) > 0)
        dbWriteTable(dbconn, 'column_names', column_map, append = TRUE, row.names = 0)
    }
    TRUE
  }

  # There have been issues with data stored incorrectly into the cache,
  # likely due to a bug with RPostgreSQL's dbWriteTable (with append = TRUE).
  # This function ensures the data was stored correctly.
  verify_integrity_of_cached_data <- function(df, tblname) {
    slices <- slice(seq_len(nrow(df)), nrow(df) / 100)
    res <- do.call("rbind", Filter(Negate(is.null), lapply(slices, function(slice) {
      dbGetQuery(dbconn, build_select_by_multiple_keys(df[slice, id_cols], tblname))
    })))
    if (!all(dim(df) == dim(res))) {
      warning("Data saved to the database had dimensions ",
           paste(dim(df), collapse = ', '), " yet data ultimately stored ",
           "had dimensions ", paste(dim(res), collapse = ', '))
      FALSE
    } else {
      if (!setequal(colnames(res), colnames(df))) {
        diffcols <- c(setdiff(colnames(res), colnames(df)),
                      setdiff(colnames(df),  colnames(res)))
        warning("Data saved to the cache and data ultimately stored did not ",
                "match because the following columns belong to the symmetric ",
                "difference: ", paste(diffcols, collapse = ', '))
      } else {
        res <- as.data.frame(stringsAsFactors = FALSE, lapply(res, as.character))
        df <- as.data.frame(stringsAsFactors = FALSE, lapply(df, as.character))
        res <- res[, colnames(df)]
        row_order <- do.call(order, as.list(df[, id_cols])) # Order by primary keys
        df <- df[row_order, ]; res <- res[row_order, ]
        if (!identical(TRUE, tmp <- all.equal(res[, colnames(df)], df))) {
          warning("Data saved to the cache and data ultimately stored were ",
                  "not all equal. Something may have gone wrong! Errors: ",
                  paste(tmp, collapse = "\n"))
        } else TRUE
      }
    }
  }

  write_column_hashed_data <- function(df, append = TRUE) {
    write_column_names_map(colnames(df))

    # Store a copy of the ID columns (ending with '_id')
    id_cols_ix <- which(is.element(colnames(df), id_cols))
    colnames(df) <- get_hashed_names(colnames(df))
    df[, id_cols] <- df[, id_cols_ix]

    # Convert some types to character so they go in the DB properly.
    to_chars <- sapply(df, function(x) is.factor(x) || is.ordered(x) || is.logical(x))
    df[, to_chars] <- lapply(df[, to_chars], as.character)

    # dbWriteTable(dbconn, tblname, df, row.names = 0, append = TRUE)
    # Believe it or not, the above does not work! RPostgreSQL seems to have a
    # bug that incorrectly serializes some kinds of data into the database.
    # Thus we must roll up our sleeves and write our own INSERT query. :-(
    number_of_records_per_insert_query <- 250
    slices <- slice(seq_len(nrow(df)), nrow(df) / number_of_records_per_insert_query)

    # If we don't do this, we will get really weird bugs with numeric things stored as character
    # For example, a loan with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20) 
    on.exit(options(old_options))

#    lapply(slices, function(slice) {
#      if (!append) dbWriteTable(dbconn, tblname, df[slice, ], row.names = 0)
#      else RPostgreSQL::postgresqlpqExec(dbconn, build_insert_query(tblname, df[slice, ]))
#    })
    for (slice in slices) {
      if (!append)  {
        dbWriteTable(dbconn, tblname, df[slice, ], row.names = 0)
        append <- TRUE
      } else {
        RPostgreSQL::postgresqlpqExec(dbconn, build_insert_query(tblname, df[slice, ]))
    }}

    #verify_integrity_of_cached_data(df, tblname)
    # To make sure the above bug is addressed, we compare the data stored in
    # the database to the data.frame.
  }

  if (!dbExistsTable(dbconn, tblname)) {
    return(write_column_hashed_data(df, append = FALSE))
  }

  one_row <- dbGetQuery(dbconn, paste("SELECT * FROM ", tblname, " LIMIT 1"))
  # Columns that are missing in database need to be created
  new_names <- get_hashed_names(colnames(df))
  # We also keep non-hashed versions of ID columns around for convenience.
  new_names <- c(new_names, id_cols)
  missing_cols <- !is.element(new_names, colnames(one_row))
  # TODO: (RK) Check reverse, that we're not missing any already-present columns
  class_map <- list(integer = 'real', numeric = 'real', factor = 'text',
                    double = 'real', character = 'text', logical = 'text')
  removes <- integer(0)
  for (index in which(missing_cols)) {
    col <- new_names[index]
    if (!all(sapply(col, nchar) > 0))
      stop("Failed to retrieve MD5 hashed column names in avant:::write_data_safely")
    # TODO: (RK) Figure out how to filter all NA columns without wrecking
    # the tables.
    if (index > length(df)) index <- col
    sql <- paste0("ALTER TABLE ", tblname, " ADD COLUMN ",
                     col, " ", class_map[[class(df[[index]])[1]]])
    suppressWarnings(dbGetQuery(dbconn, sql))
  }

  # Columns that are missing in data need to be set to NA
  missing_cols <- !is.element(colnames(one_row), new_names)
  if (sum(missing_cols) > 0) {
    raw_names <- translate_column_names(colnames(one_row)[missing_cols], dbconn)
    stopifnot(is.character(raw_names))
    df[, raw_names] <- lapply(sapply(one_row[, missing_cols], class), as, object = NA) 
  }

  write_column_hashed_data(df)
}

#' Build an INSERT query for RPostgreSQL.
#'
#' Normally, this would not have to be done manually, but believe it or not,
#' there appears to be a bug in the built-in dbWriteTable for some complex
#' data!
#'
#' @param tblname character. The table to insert into.
#' @param df data.frame. The data to insert.
#' @return a string representing the query that must be executed, to be
#'    used in conjunction with postgresqlpgExec.
build_insert_query <- function(tblname, df) {
  if (any(dim(df) == 0)) return('')
  tmp <- sapply(df, is.character)
  df[, tmp] <- lapply(df[, tmp, drop = FALSE], function(x)
    ifelse(is.na(x), 'NULL', paste0("'", gsub("'", "''", x, fixed = TRUE), "'")))

  suppressWarnings(df[is.na(df)] <- 'NULL')

  cols <- paste(colnames(df), collapse = ', ')
  values <- paste(apply(df, 1, paste, collapse = ', '), collapse = '), (')
  Ramd::pp("INSERT INTO #{tblname} (#{cols}) VALUES (#{values})")
}

#' Build a SELECT query for multiple primary keys for RPostgreSQL.
#'
#' Turn a data.frame like \code{data.frame(a = c(1, 2, NA), b = (4, NA, 5)}
#' into the query
#'  \code{"SELECT * FROM tblname WHERE (a=1 AND b=4) OR (a=2 AND b IS NULL) OR (a is NULL and b=5)"}
#' 
#' @param df data.frame. The data.frame of primary keys, with names
#'   corresponding to database column names.
#' @param tblname character. The name of the table.
build_select_by_multiple_keys <- function(df, tblname) {
  # Convert all columns to integer for safety (use as.character first
  # for correct behavior on factors and ordereds).
  df <- data.frame(lapply(df, function(x) as.integer(as.character(x))))
  # Construct an AND+OR WHERE clause of the form (col1 = 1 AND col2 = 5 AND ...) OR ...  
  cols <- paste0(colnames(df), '::integer')
  AND_logic <- apply(df, 1, function(x) paste0('(', paste(ifelse(is.na(x),
    paste(cols, 'IS NULL'), paste(cols, '=', x)), collapse = ' AND '), ')'))
  full_logic <- paste(AND_logic, collapse = ' OR ')
  Ramd::pp("SELECT * FROM #{tblname} WHERE #{full_logic}")
}

