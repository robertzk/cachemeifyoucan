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
