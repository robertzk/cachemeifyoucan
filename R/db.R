#' Database table name for a given prefix and salt.
#'
#' @name table_name
#' @param prefix character. Prefix.
#' @param salt list. Salt for the table name.
#' @importFrom digest digest
#' @return the table name. This will just be \code{"prefix_"}
#'   appended with the MD5 hash of the digest of the \code{salt}.
table_name <- function(prefix, salt) {
  tolower(paste0(prefix, "_", digest::digest(salt)))
}

#' Fetch the map of column names.
#'
#' @name column_names_map
#' @param dbconn SQLConnection. A database connection.
#' @importFrom DBI dbGetQuery
column_names_map <- function(dbconn) {
  DBI::dbGetQuery(dbconn, "SELECT * FROM column_names")
}

#' Fetch all the shards for the given table name.
#'
#' @name get_shards_for_table
#' @param dbconn SQLConnection. A database connection.
#' @param tbl_name character. The calculated table name for the function.
#' @return one or many names of the shard tables.
#' @importFrom DBI dbGetQuery
get_shards_for_table <- function(dbconn, tbl_name) {
  if (!DBI::dbExistsTable(dbconn, 'table_shard_map')) create_shards_table(dbconn, 'table_shard_map')
  DBI::dbGetQuery(dbconn, paste0("SELECT shard_name FROM table_shard_map where table_name='", tbl_name, "'"))
}

#' Fetch all the shards for the given table name.
#'
#' @name create_table
#' @param dbconn SQLConnection. A database connection.
#' @param tblname character.The table to be created
#' @importFrom DBI dbGetQuery
#' @importFrom productivus pp
create_shards_table <- function(dbconn, tblname) {
  if (DBI::dbExistsTable(dbconn, tblname)) return(TRUE)
  sql <- pp("CREATE TABLE #{tblname} (table_name varchar(255) NOT NULL, shard_name varchar(255) NOT NULL);")
  DBI::dbGetQuery(dbconn, sql)
  TRUE
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
  DBI::dbRemoveTable(dbconn, tblname)
  success <- FALSE
  df[, vapply(df, function(x) all(is.na(x)), logical(1))] <- as.character(NA)
  while (!success) {
    DBI::dbWriteTable(dbconn, tblname, df, append = FALSE, row.names = 0)
    num_rows <- DBI::dbGetQuery(dbconn, paste0('SELECT COUNT(*) FROM ', tblname))
    if (num_rows == nrow(df)) success <- TRUE
  }
}

## Helper utility for safe IO of a data.frame to a database connection.
##
## This function will be mindful of three problems: non-existent columns,
## long column names, and sharding *data.frame*s with too many columns.
##
## Since this is meant to be used as a helper function for caching
## data, we must take a few precautions. If certain variables are not
## available for older data but are introduced for newer data, we
## must be careful to create those columns first.
##
## Furthermore, certain column names may be longer than PostgreSQL supports.
## To circumvent this problem, this helper function stores an MD5
## digest of each column name and maps them using the `column_names`
## helper table.
##
## By default, this function assumes any data to be written is not
## already present in the table and should be appended. If the table does
## not exist, it will be created.
##
#' Write data.frames to DB addressing pitfalls
#'
#' @name write_data_safely
#' @param dbconn PostgreSQLConnection. The database connection.
#' @param tblname character. The table name to write the data into.
#' @param df data.frame. The data to write.
#' @param key character. The identifier column name.
#' @importFrom productivus pp
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

    ## If we don't do this, we will get really weird bugs with numeric things stored as character
    ## For example, a row with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20)
    on.exit(options(old_options))

    ## Store the map of raw to MD5'ed column names in the column_names table.
    if (!DBI::dbExistsTable(dbconn, 'column_names'))
      dbWriteTableUntilSuccess(dbconn, 'column_names', column_map)
    else {
      raw_names <- DBI::dbGetQuery(dbconn, 'SELECT raw_name FROM column_names')[[1]]
      column_map <- column_map[!is.element(column_map$raw_name, raw_names), ]
      if (NROW(column_map) > 0) {
        dbWriteTable(dbconn, 'column_names', column_map, append = TRUE, row.names = 0)
      }
    }
    TRUE
  }

  ## Input: table name and calculated shard names
  write_table_shard_map <- function(tblname, shard_names) {
    ## Example:
    ##
    ## |   | table_name  | shard_name  |
    ## |---|-------------|-------------|
    ## | 1 | tblname_1   | shard_1     |
    ## | 2 | tblname_1   | shard_2     |
    ## | 3 | tblname_2   | shard_3     |
    table_shard_map <- data.frame(table_name = rep(tblname, length(shard_names)), shard_name = shard_names)
    ## If we don't do this, we will get really weird bugs with numeric things stored as character
    ## For example, a row with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20)
    on.exit(options(old_options))

    ## Store the map of logical table names to physical shards in the table_shard_map table.
    if (!DBI::dbExistsTable(dbconn, 'table_shard_map'))
      dbWriteTableUntilSuccess(dbconn, 'table_shard_map', table_shard_map)
    else {
      shards <- DBI::dbGetQuery(dbconn, pp("SELECT shard_name FROM table_shard_map WHERE table_name='#{tblname}'"))
      if (NROW(shards) > 0) {
        shards <- shards[[1]]
        table_shard_map <- table_shard_map[table_shard_map$shard_name %nin% shards,]
      }
      if (NROW(table_shard_map) > 0) {
        dbWriteTable(dbconn, 'table_shard_map', table_shard_map, append = TRUE, row.names = 0)
      }
    }
    TRUE
  }

  get_shard_names <- function(df, tblname) {
    numcols <- NCOL(df)
    if (numcols == 0) return(FALSE)
    numshards <- ceiling(numcols / MAX_COLUMNS_PER_SHARD)
    ## All data-containing tables will end with prefix *_shard#{n}*
    paste0(tblname, "_shard", seq(numshards))
  }

  df2shards <- function(df, shard_names) {
    if (length(shard_names) == 1) {
      return(list(list(df = df, shard_name = shard_names)))
    }
  }

  write_column_hashed_data <- function(df, tblname, append = TRUE) {
    ## Create the mapping between original column names and their MD5 companions
    write_column_names_map(colnames(df))

    ## Store a copy of the ID columns (ending with '_id')
    id_cols_ix <- which(is.element(colnames(df), id_cols))
    colnames(df) <- get_hashed_names(colnames(df))
    df[, id_cols] <- df[, id_cols_ix]

    ## Convert some types to character so they go in the DB properly.
    to_chars <- unname(vapply(df, function(x) is.factor(x) || is.ordered(x) || is.logical(x), logical(1)))
    df[to_chars] <- lapply(df[to_chars], as.character)

    ## dbWriteTable(dbconn, tblname, df, row.names = 0, append = TRUE)
    ##
    ## Believe it or not, the above does not work! RPostgreSQL seems to have a
    ## bug that incorrectly serializes some kinds of data into the database.
    ## Thus we must roll up our sleeves and write our own INSERT query. :-(
    number_of_records_per_insert_query <- 250
    slices <- slice(seq_len(nrow(df)), number_of_records_per_insert_query)

    ## If we don't do this, we will get really weird bugs with numeric things stored as character
    ## For example, a row with ID 100000 will be stored as 10e+5, which is wrong.
    old_options <- options(scipen = 20, digits = 20)
    on.exit(options(old_options))

    for (slice in slices) {
      if (!append)  {
        dbWriteTableUntilSuccess(dbconn, tblname, df)
        append <- TRUE
      } else {
        insert_query <- build_insert_query(tblname, df[slice, , drop = FALSE])
        if (is(dbconn, "MonetDBConnection")) {
          DBI::dbSendQuery(dbconn, insert_query)
        } else {
          RPostgreSQL::postgresqlpqExec(dbconn, insert_query)
        }
    }}
  }

  ## Find the appropriate shards for this dataframe and tablename
  shard_names <- get_shard_names(df, tblname)
  ## Create references for these shards if needed
  write_table_shard_map(tblname, shard_names)
  ## Split the dataframe into the appropriate shards
  df_shard_map <- df2shards(df, shard_names)

  lapply(df_shard_map, function(lst) {
    tblname <- lst$shard_name
    df <- lst$df
    if (!DBI::dbExistsTable(dbconn, tblname))
      return(write_column_hashed_data(df, tblname, append = FALSE))

    one_row <- DBI::dbGetQuery(dbconn, paste("SELECT * FROM ", tblname, " LIMIT 1"))
    if (nrow(one_row) == 0) {
      DBI::dbRemoveTable(dbconn, tblname)
      return(write_column_hashed_data(df, tblname, append = FALSE))
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
      suppressWarnings(DBI::dbGetQuery(dbconn, sql))
    }

    # Columns that are missing in data need to be set to NA
    missing_cols <- !is.element(colnames(one_row), new_names)
    if (sum(missing_cols) > 0) {
      raw_names <- translate_column_names(colnames(one_row)[missing_cols], dbconn)
      stopifnot(is.character(raw_names))
      df[, raw_names] <- lapply(sapply(one_row[, missing_cols], class), as, object = NA)
    }

    write_column_hashed_data(df, tblname)
  })
  invisible(TRUE)
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
  paste0("INSERT INTO ", tblname, " (", cols, ") VALUES (", values, ")")
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
  shards <- get_shards_for_table(dbconn, tbl_name)
  ## If there are no existing shards - then nothing is cached yet
  if(NROW(shards) == 0) {
    return(ids)
  } else {
    shards <- shards[[1]]
  }
  if (!DBI::dbExistsTable(dbconn, shards[1])) return(ids)
  id_column_name <- get_hashed_names(key)
  ## We can check only the first shard because all shards have the same keys
  present_ids <- DBI::dbGetQuery(dbconn, paste0(
    "SELECT ", id_column_name, " FROM ", shards[1]))
  ## If the table is empty, a 0-by-0 dataframe will be returned, so
  ## we must be careful.
  present_ids <- if (NROW(present_ids)) present_ids[[1]] else integer(0)
  setdiff(ids, present_ids)
}

#' remove old keys to maintain uniqueness of "id" for the sake of force pushing
#'
#' @name remove_old_key
#' @param dbconn SQLConnection. The database connection.
#' @param tbl_name character. Database table name.
#' @param ids vector. A vector of ids.
#' @param key character. Identifier of database table.
#' @importFrom DBI dbExistsTable
#' @importFrom DBI dbSendQuery
remove_old_key <- function(dbconn, tbl_name, ids, key) {
  if (length(ids) == 0) return(invisible(NULL))
  if (!DBI::dbExistsTable(dbconn, tbl_name)) return(invisible(NULL))
  id_column_name <- get_hashed_names(key)
  shards <- get_shards_for_table(dbconn, tbl_name)
  if(NROW(shards) == 0) return(invisible(NULL))
  ## In this case though, we need to delete from all shards to keep them consistent
  sapply(shards, function(shard) {
    DBI::dbSendQuery(dbconn, paste0(
      "DELETE FROM ", shard, " WHERE ", id_column_name, " IN (",
      paste(ids, collapse = ","), ")"))
  })
  invisible(NULL)
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
  do.call(DBI::dbConnect, append(list(drv = DBI::dbDriver(config.database$adapter)),
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
  if (is.character(con)) {
    return(db_connection(con, env))
  } else if (is.function(con)) {
    return(con())
  } else if (length(grep("SQLConnection", class(con)[1])) > 0) {
    #print("Connection is already established")
    return(con)
  } else {
    stop("Invalid connection setup")
  }
}

#' Helper function to check the database connection.
#'
#' @param con SQLConnection.
#' @return `TRUE` or `FALSE` indicating if the database connection is good.
#' @export
is_db_connected <- function(con) {
  res <- tryCatch(fetch(DBI::dbSendQuery(con, "SELECT 1 + 1"))[1,1], error = function(e) NULL)
  if(is.null(res) || res != 2) return(FALSE)
  TRUE
}
