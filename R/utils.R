`%||%` <- function(x, y) if (is.null(x)) y else x

`%nin%` <- Negate(`%in%`)

slice <- function(x, n) split(x, as.integer((seq_along(x) - 1) / n))

verbose <- function() { isTRUE(getOption("cachemeifyoucan.verbose", FALSE)) }

## This constant determines how many columns a table can have, thus affecting sharding
MAX_COLUMNS_PER_SHARD <- 500

merge2 <- function(list_of_dataframes, id_name) {
  Reduce(function(x, y) { merge(x[c(id_name, setdiff(colnames(x), colnames(y)))], y, by = id_name) },
         list_of_dataframes)
}
