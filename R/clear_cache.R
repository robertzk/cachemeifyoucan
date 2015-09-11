#' Wipe the cachemeifyoucan caching layer for a cached function.
#'
#' @param fn function or character.
#' @param ... additional arguments to the function. If present,
#'   these should specify all the salts of the function. The tables
#'   representing the combination of this \code{fn} and parameters
#'   will be wiped. By default, if only \code{fn} is provided,
#'   wipe \emph{all} tables that cache functions whose prefix
#'   is the \code{fn}.
#' @export
#' @return \code{TRUE} or \code{FALSE} according as the wipe
#'   attempt was successful.
clear_cache <- function(fn, ...) {
  UseMethod("clear_cache")
}

#' @export
clear_cache.cached_function <- function(fn, ...) {
  clear_cache(prefix(fn))
}

#' @export
clear_cache.character <- function(fn, ...) {
}
