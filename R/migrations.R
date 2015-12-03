#' Table migrate
#'
#' @param table_list list. A list on form of
#'   \code{list("old_table_name_1" = "new_table_name_1", ..., "old_table_name_n" = "new_table_name_n")}
#' @param cached_fn cached_function.
#'
#' @export
table_migrate <- function(cached_fn, table_list) {
  stopifnot(is(cached_fn, 'cached_function'))
  ## Imagine in the future you add one more parameter to your function
  ## that you want to make part of your salt.
  ## Unfortunately, this would mean that the cache table name would change
  ## and your old cache would not be used.
  ##
  ## Enter migrations. Since everything is stored in shards anyway,
  ## all we need to do is point the new table to access the old shards.
  ## Current migration implementation requires you to fetch the correct table
  ## names by hand i.e. doing the following:
  ## ```r
  ## debugonce(cachemeifyoucan:::execute)
  ## old_cached_function(...)  # call your old cached function
  ## fcn_call$table  # this would be the old table name
  ## ```
  ## And repeating the same step for the new function.
  ## then the inputs to this migrator should be the cached function itself
  ## (for now only used for getting the right db connection)
  ## and a list in the form of
  ## `list("old_table_name_1" = "new_table_name_1", ..., "old_table_name_n" = "new_table_name_n")`

  dbconn <- environment(cached_fn)[['_con']]
  if (is.null(dbconn)) {
    stop('Please execute the cached function at least once in the current R session to initialize the cache db connection')
  }
  stopifnot(DBI::dbExistsTable(dbconn, 'table_shard_map'))  # Gotta have some shards first
  ## For every record in the table list...
  for (tblname in names(table_list)) {
    ## Get shards corresponsing to the *old* table
    shards <- DBI::dbGetQuery(dbconn, paste0("SELECT shard_name FROM table_shard_map WHERE table_name='", tblname,"'"))
    ## Do stuff only if some shards exist
    if (NROW(shards) > 0) {
      shards <- shards[[1]]
      ## For every corresponsing shard...
      for (shard in shards) {
        ## Insert a mapping from the *new* table to the shard
        DBI::dbGetQuery(dbconn,
          paste0("INSERT INTO table_shard_map values ('", table_list[[tblname]], "', '", shard, "')"))
      }
    }
  }
}

#' Utility to update the cache when the salt is changed for an existing cache.
#'
#' @param dbconn SQLConnection. A database connection.
#' @param prefix character.
#' @param old_salt list. The old salt object.
#' @param new_salt list. A new salt object.
#' @export
update_cache_salt <- function(dbconn, prefix, old_salt, new_salt) {
  ## Updating the salts of an existing cached function can be tricky. In order to do so,
  ## the old shards should be migrated over to ensure while still preserving the old shards
  ## in case a single user doesn't update cachemeifyoucan in all their machines.

  ## In this approach, we are simple copying over old data into new shards and adding new entries
  ## to the table_shard_map for the new shard table

  old_table_name <- cachemeifyoucan:::table_name(prefix, old_salt)
  new_table_name <- cachemeifyoucan:::table_name(prefix, new_salt)

  old_shard_names <- get_shards_for_table(old_table_name)
  new_shard_names <- generate_new_shard_names(new_table_name, length(old_shard_names))

  lapply(seq_along(new_shard_names), function(i) {
    message(sprintf("Copying shard from %s to %s.", old_shard_names[i], new_shard_names[i]))
    copy_shard(old_shard_names[i], new_shard_names[i])
  })

  write_table_shard_map(dbconn, new_table_name, new_shard_names)
}

copy_shard <- function(dbconn, old_shard_name, new_shard_name) {
  fields <- names(DBI::dbGetQuery(dbconn, sprintf("SELECT * FROM %s LIMIT 1", old_shard_name)))
  id_col <- grep("^(.*_id|ids)$", fields, value = TRUE)
  if (length(id_col) > 1) stop("More than one possible id column found.")
  if (length(id_col) < 1) stop("No id column found in shard. Not able to add an index")

  query <- sprintf("CREATE TABLE %s AS TABLE %s", new_shard_name, old_shard_name)
  add_index(dbconn, new_table_name, id_col)
}
