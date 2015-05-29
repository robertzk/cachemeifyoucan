A general caching layer for R projects [![Build Status](https://travis-ci.org/robertzk/cachemeifyoucan.svg?branch=master)](https://travis-ci.org/robertzk/cachemeifyoucan) [![Coverage Status](https://coveralls.io/repos/robertzk/cachemeifyoucan/badge.svg?branch=master)](https://coveralls.io/r/robertzk/cachemeifyoucan) ![Release Tag](https://img.shields.io/github/tag/robertzk/cachemeifyoucan.svg) [![Documentation](https://img.shields.io/badge/rocco--docs-%E2%9C%93-blue.svg)](http://robertzk.github.io/cachemeifyoucan/)
==========

One of the most frustrating parts about being a data scientist is
waiting for data or other large downloads. This package offers a caching
layer for arbitrary functions that relies on a database backend.

Imagine you have a function that requires network communication or
complex computation to produce some dataframe, where each row has
a primary key.

```r
fetch_amazon_reviews <- function(review_id) {
  # Return a data.frame of Amazon reviews obtained from Amazon's API.
}
```

Ideally, we should only ever have to run this function once per unique
`review_id`, since we should be able to cache the rows that have already
been computed. Using cachemeifyoucan, we can write:

```r
cached_amazon_reviews <- cachemeifyoucan::cache(
  fetch_amazon_reviews,
  key    = "review_id",
  con    = "path/to/database.yml",
  env    = "cache"
)
```

This will use the database connection specified in the database.yml file
at `path/to/database.yml` to cache the function, and *only* fetch new
records when that `review_id` has never yet been passed to
`cached_amazon_reviews`.

```yml
# path/to/database.yml
cache: # We only support postgres for now. Make sure you have instaled
       # and loaded the RPostgreSQL package.
  adapter: PostgreSQL
  host: localhost     # Or your database host
  dbname: name        # Name of the caching database  
  user: username      # The user name for the database connection 
  password: password  # The password for the database connection
```

After this setup, calling something like `cached_amazon_reviews(c(1, 2))`
twice will cause the second call to use the database caching layer, speeding
up calls that ask for already-computed primary keys.

# Installation

This package is not yet available from CRAN (as of May 29, 2015).
To install the latest development builds directly from GitHub, run this instead:

```R
if (!require("devtools")) install.packages("devtools")
devtools::install_github("robertzk/cachemeifyoucan")
```

When using the database caching features, you will need to setup a
`database.yml` file as in the example above.


# Testing

To run tests locally, you'll need the following:

* A working PostgreSQL installation and a dedicated user account, "postgres".
* A database called "travis" that user "postgres" can access.

In order to run the tests in your R console, run the following commands:

```R
test("/path/to/cachemeifyoucan")
```

# Adapters

Eventually, it will be possible to specify arbitrary caching backends: a database,
S3, a flat file. For more complex strategies, it will be a little bit of work
to set up the correct caching call, but the resulting performance
benefits should be worth it.

For now cachemeifyoucan works with PostgreSQL.
