# Version 0.1.6

  * Batchman integration now uses `robust_batch`.

# Version 0.1.4

  * Critical fix for usage of the `force.` parameter.

# Version 0.1.3

  * Integration with [batchman](http://github.com/peterhurford/batchman). If a call
    to a cached function procures uncached data of row count in excess that of the
    parameter `batch_size` to the `cachemeifyoucan::cache` call, caching progress
    will automatically happen in batches of `batch_size` (by default 100). In other
    words, data will be cached on a `batch_size` record by record basis, instead
    of all at once, which is very susceptible to failures.

    Note this feature will only trigger if the batchman package is installed.

# Version 0.1.2

  * Bug fix release.

# Version 0.1.1

  * Better support and fixes for MonetDB adapter.

# Version 0.1.0

  * Initial creation of the package.
