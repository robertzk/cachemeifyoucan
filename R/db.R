#' Database table name for a given prefix and salt.
#' 
#' @name table_name
#' @param prefix character. Prefix.
#' @param salt list. Salt for the table name.
#' @importFrom digest digest
#' @return the table name. This will just be \code{"prefix_"}
#'   appended with the MD5 hash of the digest of the \code{salt}.
table_name <- function(prefix, salt) {
  paste0(prefix, "_", digest::digest(salt))
}

#' Fetch the map of column names.
#'
#' @name column_names_map
#' @param dbconn SQLConnection. A database connection.
#' @importFrom DBI dbGetQuery
column_names_map <- function(dbconn) {
  dbGetQuery(dbconn, 'SELECT * FROM column_names')
}

#' MD5 digest of column names.
#'
#' @name get_hashed_names
#' @param raw_names character. A character vector of column names.
#' @importFrom digest digest
#' @return the character vector of hashed names.
get_hashed_names <- function(raw_names) {
  paste0('c', vapply(raw_names, digest, character(1)))
}

#' Translate column names using the column_names table from MD5 to raw.
#'
#' @name translate_column_names
#' @param names character. A character vector of column names.
#' @param dbconn SQLConnection. A database connection.
translate_column_names <- function(names, dbconn) {
  name_map <- column_names_map(dbconn)
  name_map <- setNames(as.list(name_map$raw_name), name_map$hashed_name)
  vapply(names, function(name) name_map[[name]] %||% name, character(1))
}

#' Convert the raw fetched database table to a readable data frame.
#'
#' @name db2df
#' @param df. Raw fetched database table.
#' @param dbconn SQLConnection. A database connection.
#' @param key. Identifier of database table.
db2df <- function(df, dbconn, key) {
  df[[key]] <- NULL
  colnames(df) <- translate_column_names(colnames(df), dbconn)
  df
}

#' Fetch the data frame from database conditioned on a key.
#'
#' @name read_data
#' @param dbconn SQLConnection. A database connection.
#' @param tblname character. Database table name.
#' @param ids vector. A vector of ids.
#' @param key character. Table key.
#' @importFrom DBI dbReadTable
read_data <- function(dbconn, tblname, ids, key) {
  df <- db2df(dbReadTable(dbconn, tblname), dbconn, key)
  id_col <- grep(key, colnames(df), value = TRUE)
  if (length(id_col) != 1)
    stop("The data you are reading from the database must contain exactly one ",
         paste0("column same as ", key))
  df[df[[id_col]] == ids, , drop = FALSE]
}

#' Try and check dbWriteTable until success
#'
#' @name dbWriteTableUntilSuccess
#' @param dbconn SQLConnection. A database connection.
#' @param tblname character. Database table name.
#' @param df data frame. The data frame to insert.
#' @importFrom DBI dbWriteTable
#' @importFrom DBI dbGetQuery
#' @importFrom DBI dbRemoveTable
dbWriteTableUntilSuccess <- function(dbconn, tblname, df) {
  dbRemoveTable(dbconn, tblname)
  success <- FALSE
  df[, vapply(df, pryr::compose(all, is.na), logical(1))] <- as.character(NA)
  while (!success) {
    dbWriteTable(dbconn, tblname, df, append = FALSE, row.names = 0)
    num_rows <- dbGetQuery(dbconn, paste0('SELECT COUNT(*) FROM ', tblname))
    if (num_rows == nrow(df)) success <- TRUE
  }
}

#' Helper utility for safe IO of a data.frame to a database connection.
#'
#' This function will be mindful of two problems: non-existent columns
#' and long column names.
#'
#' Since this is meant to be used as a helper function for caching 
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
#' @param key character. The identifier column name.
write_data_safely <- function(dbconn, tblname, df, key) {
  if (is.null(df)) return(FALSE)
  if (!is.data.frame(df)) return(FALSE)
  if (nrow(df) == 0) return(FALSE)

  if (missing(key)) {
    id_cols <- grep('(_|^)id$', colnames(df), value = TRUE)
    if (length(id_cols) == 0)
      stop("The data you are writing to the database must contain at least one ",
           "column ending with '_id'")
  } else 
    id_cols <- key

  write_column_names_map <- function(raw_names) {
    hashed_names <- get_hashed_names(raw_names)
    column_map <- data.frame(raw_name = raw_names, hashed_name = hashed_names)
    column_map <- column_map[!duplicated(column_map), ]

    # If we don't do this, we will get really weird bugs with numeric things stored as character
    # For example, a row with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20) 
    on.exit(options(old_options))

    # Store the map of raw to MD5'ed column names in the column_names table.
    if (!dbExistsTable(dbconn, 'column_names'))
      #dbWriteTable(dbconn, 'column_names', column_map, row.names = 0)
      dbWriteTableUntilSuccess(dbconn, 'column_names', column_map)
    else {
      raw_names <- dbGetQuery(dbconn, 'SELECT raw_name FROM column_names')[[1]]
      column_map <- column_map[!is.element(column_map$raw_name, raw_names), ]
      if (NROW(column_map) > 0)
        dbWriteTable(dbconn, 'column_names', column_map, append = TRUE, row.names = 0)
    }
    TRUE
  }

  write_column_hashed_data <- function(df, append = TRUE) {
    write_column_names_map(colnames(df))

    # Store a copy of the ID columns (ending with '_id')
    id_cols_ix <- which(is.element(colnames(df), id_cols))
    colnames(df) <- get_hashed_names(colnames(df))
    df[, id_cols] <- df[, id_cols_ix]

    # Convert some types to character so they go in the DB properly.
    to_chars <- vapply(df, function(x) is.factor(x) || is.ordered(x) || is.logical(x), logical(1))
    df[, to_chars] <- lapply(df[, to_chars], as.character)

    # dbWriteTable(dbconn, tblname, df, row.names = 0, append = TRUE)
    # Believe it or not, the above does not work! RPostgreSQL seems to have a
    # bug that incorrectly serializes some kinds of data into the database.
    # Thus we must roll up our sleeves and write our own INSERT query. :-(
    number_of_records_per_insert_query <- 250
    slices <- slice(seq_len(nrow(df)), number_of_records_per_insert_query)

    # If we don't do this, we will get really weird bugs with numeric things stored as character
    # For example, a row with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20) 
    on.exit(options(old_options))

    for (slice in slices) {
      if (!append)  {
        #dbWriteTable(dbconn, tblname, df[slice, , drop = FALSE], append = append, row.names = 0)
        dbWriteTableUntilSuccess(dbconn, tblname, df)
        append <- TRUE
      } else {
        RPostgreSQL::postgresqlpqExec(dbconn, 
          build_insert_query(tblname, df[slice, , drop = FALSE]))
    }}
  }

  if (!dbExistsTable(dbconn, tblname)) 
    return(write_column_hashed_data(df, append = FALSE))

  one_row <- dbGetQuery(dbconn, paste("SELECT * FROM ", tblname, " LIMIT 1"))
  if (nrow(one_row) == 0) {
    dbRemoveTable(dbconn, tblname)
    return(write_column_hashed_data(df, append = FALSE))
  }

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
    if (!all(vapply(col, nchar, integer(1)) > 0))
      stop("Failed to retrieve MD5 hashed column names in write_data_safely")
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
  tmp <- vapply(df, is.character, logical(1))
  df[, tmp] <- lapply(df[, tmp, drop = FALSE], function(x)
    ifelse(is.na(x), 'NULL', paste0("'", gsub("'", "''", x, fixed = TRUE), "'")))

  suppressWarnings(df[is.na(df)] <- 'NULL')

  cols <- paste(colnames(df), collapse = ', ')
  values <- paste(apply(df, 1, paste, collapse = ', '), collapse = '), (')
  paste0("INSERT INTO ", tblname, " ", cols, ") VALUES (", values, ")")
}

#' setdiff current ids with those in the table of the database.
#'
#' @name get_new_key
#' @param dbconn SQLConnection. The database connection.
#' @param tbl_name character. Database table name.
#' @param ids vector. A vector of ids.
#' @param key character. Identifier of database table.
#' @importFrom DBI dbExistsTable
#' @importFrom DBI dbGetQuery
get_new_key <- function(dbconn, tbl_name, ids, key) {
  if (length(ids) == 0) return(integer(0))
  if (!DBI::dbExistsTable(dbconn, tbl_name)) return(ids)
  id_column_name <- get_hashed_names(key)
  present_ids <- DBI::dbGetQuery(dbconn, paste0(
    "SELECT ", id_column_name, " FROM ", tbl_name))[[1]]
  setdiff(ids, present_ids)
}

#' Obtain a connection to a database.
#'
#' By default, this function will read from the `cache` environment.
#'
#' Your database.yml should look like:
#'
#' development:
#'   adapter: PostgreSQL
#'   username: <username>
#'   password: <password>
#'   database: <name>
#'   host: <domain>
#'   port: <port #>
#'
#' @param database.yml character. The location of the database.yml file
#'   to use. This could, for example, some file in the config directory.
#' @param env character. What environment to use. The default is
#'   `"cache"`.
#' @param verbose logical. Whether or not to print messages indicating
#'   loading is in progress.
#' @return the database connection.
#' @export
db_connection <- function(database.yml, env = "cache",
                          verbose = TRUE, strict = TRUE) {
  if (is.null(database.yml)) { if (strict) stop('database.yml is NULL') else return(NULL) }
  if (!file.exists(database.yml)) {
    if (strict) stop("Provided database.yml file does not exist: ", database.yml)
    else return(NULL)
  }

  if (verbose) message("* Loading database connection...\n")
  database.yml <- paste(readLines(database.yml), collapse = "\n")
  config.database <- yaml::yaml.load(database.yml)
  if (!missing(env) && !is.null(env)) {
    if (!env %in% names(config.database))
      stop(paste0("Unable to load database settings from database.yml ",
              "for environment '", env, "'"))
    config.database <- config.database[[env]]
  } else if (missing(env) && length(config.database) == 1) {
    config.database <- config.database[[1]]
  }
  # Authorization arguments needed by the DBMS instance
  # TODO: (RK) Inform user if they forgot database.yml entries.
  do.call(DBI::dbConnect, append(list(drv = dbDriver(config.database$adapter)), 
    config.database[!names(config.database) %in% "adapter"]))
}

#' Helper function to build the connection.
#'
#' @param con connection. Could be characters of yaml file path with optional environment,
#'   or a function passed by user to establish the connection, or a database connetion object.
#' @param env character. What environment to use when `con` is a the yaml file path.
#' @return a list of database connection and if it can be re-established.
#' @export
build_connection <- function(con, env) {
  re <- TRUE
  if (is.character(con)) {
    con <- db_connection(con, env)
  } else if (is.function(con)) {
    con <- con()
  } else if (length(grep("SQLConnection", class(con)[1])) > 0) {
    #print("Connection is already established")
    re <- FALSE
  } else { 
    stop("Invalid connection setup")
  }
  list(con = con, re = re)
}

#' Helper function to check the database connection.
#' 
#' @param con SQLConnection.
#' @return `TRUE` or `FALSE` indicating if the database connection is good.
#' @export
is_db_connected <- function(con) {
  res <- tryCatch(fetch(dbSendQuery(con, "SELECT 1 + 1"))[1,1], error = function(e) NULL)
  if(is.null(res) || res != 2) return(FALSE)
  TRUE
}
