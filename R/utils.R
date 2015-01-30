`%||%` <- function(x, y) if (is.null(x)) y else x

slice <- function(x, n) split(x, as.integer((seq_along(x) - 1) / n))

