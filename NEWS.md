# Version 0.2.1

* Debug mode, triggered by `cachemeifyoucan.debug` option. Set it to true
  to see the gory metadata internals of underlying postgres tables.

# Version 0.2.0.10

* Ensure uniqueness of the response of the caching layer.

# Version 0.2.0.9

* On second thought, don't store the shards in legacy cache
  warnings in globalenv for debugging, since they could be huge.

# Version 0.2.0.8

* Introduce some changes to the `merge2` helper that will ensure
  legacy caches that have discrepancy between shards will not
  truncate their output.

# Version 0.2.0.7

 * Added cache migrations and slightly refactored how new shards are created.

# Version 0.2.0.6

 * Important bugfix for sharding.

# Version 0.2.0.5

 * Replace RPostgreSQL with rstats-db/RPostgres.

# Version 0.2.0.4

 * Fix an issue with invalid index names.

# Version 0.2.0.3

 * Fixed some bugs and added new ones.

# Version 0.2.0.2

 * Re-define `uncached` as simply stripping the caching layer, after some
   confusion as to what the correct definition should be.

# Version 0.2.0.1

  * Fixed a bug in `uncached`.

# Version 0.2.0

  * Added column sharding, which allows to store data frames with large number of columns.
  * Rocco documentation.
  * Automatically create indexes on shards, which greatly improves read times.

# Version 0.1.7

  * Integrated batchman v1.0.0.9000, which fixes robust functionality.

# Version 0.1.6.2

  * If two users are populating the caching layer at the same time, the uncached
    function will not be run twice by both functions. Instead, each attempt to
    run the uncached function will query the database for whether those records
    have been cached. This leads to a speed-up during parallel cache population.

# Version 0.1.6.1

  * Added `cachemeifyoucan.verbose` global option for whether to display
    batch caching progress.

# Version 0.1.6

  * Batchman integration now uses `robust_batch`.

# Version 0.1.5

  * Simple generic bug fix.

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
