without_rownames <- function(df) { row.names(df) <- NULL; df }

read_df <- function(conn, id_key, keys, shard) {
  sql <- paste("SELECT * FROM", shard, "WHERE", id_key, "IN (",
               paste(sanitize_sql(keys), collapse = ', '), ")")
  db2df(dbGetQuery(conn, sql),
  conn, id_key)
}
