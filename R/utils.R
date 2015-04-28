`%||%` <- function(x, y) if (is.null(x)) y else x

slice <- function(x, n) split(x, as.integer((seq_along(x) - 1) / n))

verbose <- function() { isTRUE(getOption("cachemeifyoucan.verbose", FALSE)) }

# This constant determines how many columns a table can have, thus affecting sharding
MAX_COLUMNS <- 500
