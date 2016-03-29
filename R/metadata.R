
CACHE_METADATA_TABLE <- "cache_metadata"

track_cache_salt <- function(dbconn, table_name, salt) {
}

#track_cache_salt_memoised <- memoise::memoise(track_cache_salt)


get_cache_table_salt <- function(dbconn, table_names) {
}

get_cache_meta_data <- function(dbconn, table_names) {
}

serialize_to_string <- function(obj) {
  rawToChar(serialize(obj, NULL, ascii = TRUE))
}

unserialize_from_string <- function(obj_str) {
  unserialize(charToRaw(obj_str))
}

ensure_cache_metadata_table_exists <- function(dbconn) {
}
