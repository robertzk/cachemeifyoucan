A general caching layer for R projects [![Build Status](https://travis-ci.org/robertzk/cachemeifyoucan.svg?branch=master)](https://travis-ci.org/robertzk/cachemeifyoucan) [![Coverage Status](https://coveralls.io/repos/robertzk/cachemeifyoucan/badge.svg?branch=master)](https://coveralls.io/r/robertzk/cachemeifyoucan)
==========

One of the most frustrating parts about being a data scientist is
waiting for data or other large downloads. This package offers a caching
layer for arbitrary functions that relies on a database backend.

Have some computationally expensive function? If you have [set up this package
correctly](#installation), you should be able to simply write:

```R
expensive_function <- cache(expensive_function)
```

The result is that if an atomic vector is passed as the first argument to
`expensive_function`, and the function has already computed output
for some of these values in the past, they will be retrieved from the cache.

For example, if the output of the function below is a dataframe whose first
column contains the `user_ids`, then these rows will be `rbind`ed to the
dataframe generated on any subsequent calls that ask for some users
whose analysis has already been computed.

```R
analyze_users <- function(user_ids, analysis_type) { ... }
```

The cache will keep track separately of each `analysis_type`.

# Installation

This package is not yet available from CRAN (as of Oct 10, 2014).
To install the latest development builds directly from GitHub, run this instead:

```R
if (!require("devtools")) install.packages("devtools")
devtools::install_github("robertzk", "cachemeifyoucan")
```

When using the database caching features, you will need to setup a 
`database.yml` file (I am working on writing up how to do this).

# Adapters

It will be possible to specify arbitrary caching backends: a database,
S3, a flat file. For more complex strategies, it will be a little bit of work
to set up the correct caching call, but the resulting performance
benefits should be worth it.

# Testing

To run tests locally, you'll need the following:

* A working PostgreSQL installation and a dedicated user account, "postgres".
* A database called "travis" that user "postgres" can access.

In order to run the tests in your R console, run the following commands:

```R
test("/path/to/cachemeifyoucan")
```
