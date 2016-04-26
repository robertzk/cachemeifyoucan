#' Update the cache when the salt is changed for an existing cache.
#'
#' Updating the salts of an existing cached function can be tricky. This utility
#' handles that by copying over the cached data into new shards, add indexes, and
#' add new entires into the table_shard_map.
#'
#' Note: The old shards aren't removed from the database but they can be safely
#' removed once cached functions using the old salts are fully depricated.
#'
#' @param dbconn SQLConnection. A database connection.
#' @param prefix character. The prefix for the cached_fcn
#' @param old_salt list. The old salt object.
#' @param new_salt list. A new salt object.
#' @export
update_cache_salt <- function(dbconn, prefix, old_salt, new_salt) {

  old_table_name <- cachemeifyoucan:::get_table_name(prefix, old_salt)
  new_table_name <- cachemeifyoucan:::get_table_name(prefix, new_salt)

  old_shard_names <- get_shards_for_table(dbconn, old_table_name)
  new_shard_names <- generate_new_shard_names(new_table_name, length(old_shard_names))

  lapply(seq_along(new_shard_names), function(i) {
    message(sprintf("Copying shard from %s to %s.", old_shard_names[i], new_shard_names[i]))
    copy_shard(dbconn, old_shard_names[i], new_shard_names[i])
  })

  write_table_shard_map(dbconn, new_table_name, new_shard_names)
}

copy_shard <- function(dbconn, old_shard_name, new_shard_name) {
  if (!DBI::dbExistsTable(dbconn, old_shard_name)) stop("Old shard doesn't exist.  There's nothing to migrate.")
  if (DBI::dbExistsTable(dbconn, new_shard_name)) stop("Shard table already exists. Don't want to copy over existing data.")

  fields <- names(DBI::dbGetQuery(dbconn, sprintf("SELECT * FROM %s LIMIT 1", old_shard_name)))
  id_col <- grep("^(.*_id|ids?)$", fields, value = TRUE)
  if (length(id_col) > 1) stop("More than one possible id column found.")
  if (length(id_col) < 1) stop("No id column found in shard. Not able to add an index")

  query <- sprintf("CREATE TABLE %s AS TABLE %s", new_shard_name, old_shard_name)
  DBI::dbGetQuery(dbconn, query)
  add_index(dbconn, new_shard_name, id_col)
}
