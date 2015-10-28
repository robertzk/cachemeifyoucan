#' Database table name for a given prefix and salt.
#'
#' @param prefix character. Prefix.
#' @param salt list. Salt for the table name.
#' @return the table name. This will just be \code{"prefix_"}
#'   appended with the MD5 hash of the digest of the \code{salt}.
table_name <- function(prefix, salt) {
  tolower(paste0(prefix, "_", digest::digest(salt)))
}

#' Fetch the map of column names.
#'
#' @param dbconn SQLConnection. A database connection.
column_names_map <- function(dbconn) {
  DBI::dbGetQuery(dbconn, "SELECT * FROM column_names")
}

#' Fetch all the shards for the given table name.
#'
#' @param dbconn SQLConnection. A database connection.
#' @param tbl_name character. The calculated table name for the function.
#' @return one or many names of the shard tables.
get_shards_for_table <- function(dbconn, tbl_name) {
  if (!DBI::dbExistsTable(dbconn, 'table_shard_map')) create_shards_table(dbconn, 'table_shard_map')
  DBI::dbGetQuery(dbconn, paste0("SELECT shard_name FROM table_shard_map where table_name='", tbl_name, "'"))$shard_name
}

#' Create the table <=> shards map.
#'
#' @rdname create_table
#' @param dbconn SQLConnection. A database connection.
#' @param tblname character.The table to be created
create_shards_table <- function(dbconn, tblname) {
  if (DBI::dbExistsTable(dbconn, tblname)) return(TRUE)
  sql <- paste0("CREATE TABLE ", tblname, " (table_name varchar(255) NOT NULL, shard_name varchar(255) NOT NULL);")
  DBI::dbGetQuery(dbconn, sql)
  TRUE
}

#' MD5 digest of column names.
#'
#' @param raw_names character. A character vector of column names.
#' @return the character vector of hashed names.
get_hashed_names <- function(raw_names) {
  paste0('c', vapply(raw_names, digest::digest, character(1)))
}

#' Translate column names using the column_names table from MD5 to raw.
#'
#' @param names character. A character vector of column names.
#' @param dbconn SQLConnection. A database connection.
translate_column_names <- function(names, dbconn) {
  name_map <- column_names_map(dbconn)
  name_map <- setNames(as.list(name_map$raw_name), name_map$hashed_name)
  vapply(names, function(name) name_map[[name]] %||% name, character(1))
}

#' Convert the raw fetched database table to a readable data frame.
#'
#' @param df. Raw fetched database table.
#' @param dbconn SQLConnection. A database connection.
#' @param key. Identifier of database table.
db2df <- function(df, dbconn, key) {
  df[[key]] <- NULL
  colnames(df) <- translate_column_names(colnames(df), dbconn)
  df
}

#' Create index on a table
#'
#' @name db2df
#' @param df. Raw fetched database table.
#' @param dbconn SQLConnection. A database connection.
#' @param key. Identifier of database table.
add_index <- function(dbconn, tblname, key, idx_name) {
  if (!tolower(substring(idx_name, 1, 1)) %in% letters) {
    stop(sprintf("Invalid index name '%s': must begin with an alphabetic character",
                 idx_name))
  }
  DBI::dbGetQuery(dbconn, paste0('CREATE INDEX ', idx_name, ' ON ', tblname, '(', key, ')'))
  TRUE
}

#' Try and check dbWriteTable until success
#'
#' @param dbconn SQLConnection. A database connection.
#' @param tblname character. Database table name.
#' @param df data frame. The data frame to insert.
dbWriteTableUntilSuccess <- function(dbconn, tblname, df, append = FALSE, row.names = NA) {
  if (DBI::dbExistsTable(dbconn, tblname) && !isTRUE(append)) {
    DBI::dbRemoveTable(dbconn, tblname)
  }
  if (any(is.na(df))) {
    df[, vapply(df, function(x) all(is.na(x)), logical(1))] <- as.character(NA)
  }

  repeat {
    class_map <- list(integer = 'bigint', numeric = 'double precision', factor = 'text',
                      double = 'double precision', character = 'text', logical = 'text')
    field_types <- sapply(sapply(df, class), function(klass) class_map[[klass]])
    DBI::dbWriteTable(dbconn, tblname, df, append = append,
                      row.names = row.names, field.types = field_types)
    #TODO(kirill): repeat maximum of N times
    if (!isTRUE(append)) {
      num_rows <- DBI::dbGetQuery(dbconn, paste0('SELECT COUNT(*) FROM ', tblname))
      if (num_rows == nrow(df)) break
    } else break
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
#' @param dbconn PostgreSQLConnection. The database connection.
#' @param tblname character. The table name to write the data into.
#' @param df data.frame. The data to write.
#' @param key character. The identifier column name.
#' @inheritParams cache
write_data_safely <- function(dbconn, tblname, df, key, safe_columns, blacklist) {
  if (is.null(df)) return(FALSE)
  if (!is.data.frame(df)) return(FALSE)
  if (nrow(df) == 0) return(FALSE)

  if (missing(key)) {
    id_cols <- grep('(_|^)id$', colnames(df), value = TRUE)
    if (length(id_cols) == 0)
      stop("The data you are writing to the database must contain at least one ",
           "column ending with '_id'")
  } else {
    id_cols <- key
    if (!is.integer(df[[key]]) && is.numeric(df[[key]])) {
      # TODO: (RK) Check if coercion is possible.
      df[[key]] <- as.integer(df[[key]])
    }
  }

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
      dbWriteTableUntilSuccess(dbconn, 'column_names', column_map, append = FALSE)
    else {
      raw_names <- DBI::dbGetQuery(dbconn, 'SELECT raw_name FROM column_names')[[1]]
      column_map <- column_map[!is.element(column_map$raw_name, raw_names), ]
      if (NROW(column_map) > 0) {
        if (isTRUE(safe_columns)) {
          stop("Safe Columns Error: Your function call is adding additional ",
            "columns to a cache that already has pre-existing columns. This ",
            "would suggest your cache is invalid and you should wipe the cache ",
            "and start over.")
        } else if (is.function(safe_columns)) {
          safe_columns()
        } else { # Write additional columns
          dbWriteTable(dbconn, 'column_names', column_map, append = TRUE, row.names = FALSE)
        }
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
    if (!DBI::dbExistsTable(dbconn, 'table_shard_map')) {
      dbWriteTableUntilSuccess(dbconn, 'table_shard_map', table_shard_map, append = FALSE)
    } else {
      shards <- get_shards_for_table(dbconn, tblname)
      if (length(shards) > 0) {
        table_shard_map <- table_shard_map[table_shard_map$shard_name %nin% shards, ]
      }
      if (NROW(table_shard_map) > 0) {
        DBI::dbWriteTable(dbconn, 'table_shard_map', table_shard_map, append = TRUE, row.names = FALSE)
      }
    }
    TRUE
  }

  get_shard_names <- function(df, tblname) {
    ## Two cases: the shards already exist - or they don't
    ##
    ## Fetch existing shards
    shards <- get_shards_for_table(dbconn, tblname)

    ## come up with new shards if needed
    numcols <- NCOL(df)
    if (numcols == 0) return(NULL)
    numshards <- ceiling(numcols / MAX_COLUMNS_PER_SHARD)
    ## All data-containing tables will start with prefix *shard#{n}_*
    newshards <- paste0("shard", seq(numshards), "_", digest::digest(tblname))
    if (length(shards) > 0) {
      ## only generate new shard names for shards that don't exist!
      unique(c(shards, newshards[-seq(length(shards))]))
    } else newshards
  }

  df2shards <- function(dbconn, df, shard_names, key) {
    ## Here comes the hard part. Sharding strategies!
    ##
    ## Here is how we're going to do it.
    ## We sort the shardnames, to ensure that the first shard is the biggest in size
    ## This way appending to a shard is trivial: if we have any columns in the
    ## dataframe that are not yet stored in the cache - just append them to the
    ## last shard!
    ## Since we've done the calculation of number of shards beforehand we
    ## don't even have to worry about creating new shards if something won't fit.
    ##
    ## Because it will.

    ## Make sure we don't store `key` in the used_columns! Need it in every dataframe
    used_columns <- c()

    ## We want to sort our shards prior to writing.
    ## Unfortunately, `sort(1:11) == c(1, 10, 11, 2, 3, ...)` which is not what we want
    ## That's why we're using a slightly more ghetto solution
    suffix <- strsplit(shard_names[1], '_')[[1]][2]
    lapply(paste0('shard', seq(length(shard_names)), '_', suffix), function (shard, last, key) {
      ## We need to create a map in the form of
      ## ```list(df = dataframe, shard_name = shard_names)```, where the dataframe is a subset
      ## of the original dataframe that contains less columns than
      ## **MAX_COLUMNS_PER_SHARD**.
      ## This is what we should do for each shard:
      ##
      ## 1. Determine which columns are already being stored in the shard
      ## 2. Take the subset of the dataframe that has these columns, assign it to a shard
      ## 3. See which columns are left unsaved, and add those to the last shard
      if (shard == last) {
        ## Write out the rest of the dataframe into the last shard
        list(df = df[setdiff(colnames(df), used_columns)], shard_name = shard)
      } else {
        ## If the response is empty, write the first N columns of the dataframe
        ## Otherwise, only write out those columns that already exist in this shard
        shard_exists <- DBI::dbExistsTable(dbconn, shard)
        if (isTRUE(shard_exists)) {
          one_row <- DBI::dbGetQuery(dbconn, paste0("SELECT * FROM ", shard, " LIMIT 1"))
        } else one_row <- NULL
        ## Here we abuse the fact that ```NROW(NULL) == 0```
        if (NROW(one_row) == 0 || NCOL(one_row) == 2) {
          ## This is very hacky...
          ## If we see only two columns in a shard, it means that we only stored
          ## the id and the hashed id. So basically this shard is useless!
          ## In this case we should drop it, and pretend this table doesn't exist
          if (NCOL(one_row) == 2) {
            DBI::dbGetQuery(dbconn, paste0("DROP TABLE ", shard))
          }
          columns <- colnames(df)
          columns <- columns[columns != key]
          columns <- setdiff(columns, used_columns)
          columns <- c(columns[1:MAX_COLUMNS_PER_SHARD - 1], key)
          used_columns <<- append(used_columns, columns[columns != key])
          list(df = df[columns], shard_name = shard)
        } else {
          columns <- unique(translate_column_names(colnames(one_row), dbconn))
          used_columns <<- append(used_columns, columns[columns != key])
          list(df = df[colnames(df) %in% columns], shard_name = shard)
        }
      }
    }, last = shard_names[length(shard_names)], key = key)
  }

  write_column_hashed_data <- function(df, tblname, append = TRUE) {
    ## Don't cache anything that is in the blacklist.
    if (!is.null(blacklist)) {
      if (any(is.na(blacklist))) {
        df <- df[apply(df, 1, function(x) all(!(is.na(x)))),]
      }
      df <- df[apply(df, 1, function(x) all(!(x %in% blacklist))),]
    }

    ## Create the mapping between original column names and their MD5 companions
    write_column_names_map(colnames(df))

    ## Store a copy of the ID columns (ending with '_id')
    id_cols_ix <- which(is.element(colnames(df), id_cols))
    colnames(df) <- get_hashed_names(colnames(df))
    df[, id_cols] <- df[, id_cols_ix]

    ## Convert some types to character so they go in the DB properly.
    to_chars <- unname(vapply(df, function(x) is.factor(x) || is.ordered(x) || is.logical(x), logical(1)))
    df[to_chars] <- lapply(df[to_chars], as.character)

    ## Write out to postgres
    dbWriteTableUntilSuccess(dbconn, tblname, df, row.names = FALSE, append = append)
  }

  ## Use transactions!
  DBI::dbGetQuery(dbconn, 'BEGIN')
  tryCatch({
    ## Find the appropriate shards for this dataframe and tablename
    shard_names <- get_shard_names(df, tblname)
    ## Create references for these shards if needed
    write_table_shard_map(tblname, shard_names)
    ## Split the dataframe into the appropriate shards
    df_shard_map <- df2shards(dbconn, df, shard_names, key)

    ## Actually write the data to the database
    lapply(df_shard_map, function(lst) {
      tblname <- lst$shard_name
      df <- lst$df
      if (!DBI::dbExistsTable(dbconn, tblname)) {
        ## The shard doesn't exist yet. Let's create it and index it by key!
        write_column_hashed_data(df, tblname, append = FALSE)
        add_index(dbconn, tblname, key, paste0("idx_", digest::digest(tblname)))
        return(invisible(TRUE))
      }

      one_row <- if (DBI::dbExistsTable(dbconn, tblname)) {
        DBI::dbGetQuery(dbconn, paste("SELECT * FROM ", tblname, " LIMIT 1"))
      } else NULL
      if (NROW(one_row) == 0) {
        ## The shard is empty! Delete it and write to it, finally
        ## Also, it's a great opportunity to enforce indexes on this table!
        if (DBI::dbExistsTable(dbconn, tblname))
          DBI::dbRemoveTable(dbconn, tblname)
        write_column_hashed_data(df, tblname, append = FALSE)
        add_index(dbconn, tblname, key, paste0("i", digest::digest(tblname)))
        return(invisible(TRUE))
      }

      ## Columns that are missing in database need to be created
      new_names <- get_hashed_names(colnames(df))
      ## We also keep non-hashed versions of ID columns around for convenience.
      new_names <- c(new_names, id_cols)
      missing_cols <- !is.element(new_names, colnames(one_row))
      # TODO: (RK) Check reverse, that we're not missing any already-present columns
      class_map <- list(integer = 'bigint', numeric = 'double precision', factor = 'text',
                        double = 'double precision', character = 'text', logical = 'text')
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

      ## Columns that are missing in data need to be set to NA
      missing_cols <- !is.element(colnames(one_row), new_names)
      if (sum(missing_cols) > 0) {
        raw_names <- translate_column_names(colnames(one_row)[missing_cols], dbconn)
        stopifnot(is.character(raw_names))
        df[, raw_names] <- lapply(sapply(one_row[, missing_cols], class), as, object = NA)
      }

      write_column_hashed_data(df, tblname, append = TRUE)
      DBI::dbGetQuery(dbconn, 'COMMIT')
    })
    },
    warning = function(w) {
      message("An warning occured:", w)
      message("Rollback!")
      DBI::dbRollback(dbconn)
      DBI::dbGetQuery(dbconn, 'COMMIT')
    },
    error = function(e) {
      if (grepl("Safe Columns Error", conditionMessage(e))) {
        stop(e)
      } else {
        message("An error occured:", e)
        message("Rollback!")
        DBI::dbRollback(dbconn)
        DBI::dbGetQuery(dbconn, 'COMMIT')
      }
    }
  )
  invisible(TRUE)
}

#' setdiff current ids with those in the table of the database.
#'
#' @param dbconn SQLConnection. The database connection.
#' @param tbl_name character. Database table name.
#' @param ids vector. A vector of ids.
#' @param key character. Identifier of database table.
get_new_key <- function(dbconn, tbl_name, ids, key) {
  if (length(ids) == 0) return(integer(0))
  shards <- get_shards_for_table(dbconn, tbl_name)
  ## If there are no existing shards - then nothing is cached yet
  if (length(shards) == 0) return(ids)

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
#' @param dbconn SQLConnection. The database connection.
#' @param tbl_name character. Database table name.
#' @param ids vector. A vector of ids.
#' @param key character. Identifier of database table.
remove_old_key <- function(dbconn, tbl_name, ids, key) {
  if (length(ids) == 0) return(invisible(NULL))
  id_column_name <- get_hashed_names(key)
  shards <- get_shards_for_table(dbconn, tbl_name)
  if (length(shards) == 0) return(invisible(NULL))
  ## In this case though, we need to delete from all shards to keep them consistent
  sapply(shards, function(shard) {
    DBI::dbGetQuery(dbconn, paste0(
      "DELETE FROM ", shard, " WHERE ", id_column_name, " IN (",
      paste(ids, collapse = ","), ")"))
  })
  invisible(NULL)
}
