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
  ## We use the `eval(substitute(alist(...)))` trick to get the
  ## *unevaluated* `...` arguments. For example, if we had done
  ## `eval(substitute(list(...)))` and called this function
  ## with `clear_cache(identity, foo = 1, bar = 2 + 2)`, then
  ## the result would have been `list(foo = 1, bar = 4)`.
  ##
  ## Using `eval(substitute(alist(...)))` keeps the
  ## [unevaluated list](https://stat.ethz.ch/R-manual/R-devel/library/base/html/list.html),
  ## and we would get `list(foo = 1, bar = quote(2 + 2))` --
  ## in other words, we would know that the *expression* passed
  ## as the `bar` argument was `2 + 2`, not only its final
  ## evaluated value. Most languages do not offer this
  ## flexibility, but in R everything is a [promise](https://en.wikipedia.org/wiki/Futures_and_promises)
  ## and we can access not only the *values* passed to
  ## functions, but also exactly how those values were computed, i.e.,
  ## the expression passed to the argument.
  ##
  ## In this case, we are checking if there is at least one `...` argument
  ## and we use `alist` to avoid evaluating the arguments straight away.
  if (length(eval(substitute(alist(...)))) > 0) {
    # TODO: (RK) Support partial salts by storing JSON in shards map table.
  }
}

