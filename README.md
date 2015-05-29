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

# Further examples and features

These examples assume you have a database connection object
(as specified in the DBI package) in a local variable `con`.

Imagine we have a function that returns a data.frame of information
about IMDB titles through their API. It takes an integer vector of
IDs and returns a data.frame with an "id" column, with one row for
each title. (for example, 111161 would correspond to
[The Shawkshank Redemption](http://www.imdb.com/title/tt111161/)).

```r
amazon_info <- function(id) {
  # Call external API.
}
```

Sending HTTP requests to Amazon and waiting for the response is
computationally intensive, so if we ask for some IDs that have
already been computed in the past, it would be useful to not
make additional HTTP requests for those records. For example,
we may want to do some processing on all Amazon titles. However,
new records are created each day. Instead of parsing all
the historical records on each execution, we would like to only
parse new records; old records would be retrieved from a database
table that had the same column names as a typical output data.frame
of the `amazon_info` function.

```r
cached_amazon_info <- cachemeifyoucan::cache(amazon_info, key = 'id', con = con)
```

By using the `cache` function, we are asking for the following:

  1. If we call `cached_amazon_info` with a vector of integer IDs,
     take the subset of IDs that have already been returned from
     a previous call to `cached_amazon_info`. Retrieve the data.frame
     for these records from an underlying database table.
  2. The remaining IDs (those we have never passed to `cached_amazon_info`
     should be fed to the base `amazon_info` function as if we had
     called it with this subset. This will yield another data.frame that
     was computed using live HTTP requests.

The `cached_amazon_info` function will return the union (`rbind`) of these
two data sets as one single data set, as if we had called `amazon_info`
by itself. It will also cache the second data set so another identical
call to `cached_amazon_info` will not trigger any additional HTTP requests.

## Salts

Imagine our `amazon_info` function is slightly more complicated:
instead of always returning the same information about film titles,
it has an additional parameter `type` that controls whether we
want info about the filmography or about the reviews. The output
of this function will still be data.frame's with an `id` column
and one row for each title, but the other columns can be different
now depending on the `type` parameter.

```r
amazon_info2 <- function(id, type = 'filmography') {
  if (identical(type, 'filmography')) { return(amazon_info(id)) }
  else { return(review_amazon_info(id)) } # Assume we have this other function
}
```

If we wish to cache `amazon_info2`, we need to use different underlying
database tables depending on the given `type`. One table may have
columns like `num_actors` or `film_length` and the other may have
columns such as `num_reviews` and `avg_rating`.

```r
cached_amazon_info2 <- cachemeifyoucan::cache(amazon_info2, key = 'id',
  salt = 'type', con = con)
```

We have told the caching layer to use the `type` parameter as the "salt".
This means different values of `type` will use different underlying
database tables for caching. It is up to the user to construct a
function like `amazon_info2` well so that it always returns a data.frame
with exactly the same column names if the `type` parameter is held fixed.

The salt should usually consist of a collection of parameters (typically
only one, `type` as in this example) that have a small number of possible
values; otherwise, many database tables would be created for different
values of the salt. Consider the following example.

```r
bad_amazon_filmography <- function(id, actor_id) {
  # Given a single actor_id and a vector of title IDs,
  # return information about that actor's role in the film.
}

bad_cached_amazon_filmography <-
  cachemeifyoucan::cache(bad_amazon_filmography, key = 'id',
    salt = 'actor_id', con = con)
```

We will now be creating a separate table each time we call
`bad_amazon_filmography` for a different actor!

## Prefixes

It is very important to give the function you are caching a prefix:
when it is stored in the database, its table name will be the prefix
combined with some string derived from the values in the salt.

```r
cached_review_amazon_info <- cachemeifyoucan::cache(review_amazon_info,
  key = 'id', con = con)
```

Remember our `review_amazon_info` function from an earlier example?
If we attempted to cache it without a prefix while also caching
the vanilla `amazon_info` function, the same database table would be
used for both functions! Since function representation in R is complex
and there is no good way in general to determine whether two functions
are identical, it is up to the user to determine a good prefix for
their function (usually the function's name) so that it does not clash
with other database tables.

```r
cached_amazon_info <- cachemeifyoucan::cache(amazon_info,
  prefix = 'amazon_info', key = 'id', con = con)
cached_review_amazon_info <- cachemeifyoucan::cache(review_amazon_info,
  prefix = 'review_amazon_info', key = 'id', con = con)
```

We will now use different database tables for these two functions.

## force.

`force.` is a reserved argument for the to-be-cached function. If
it is specified to be `TRUE`, the caching layer will forcibly
repopulate the database tables for the given ids. The default value
is `FALSE`.

```r
cached_amazon_info <- cachemeifyoucan::cache(amazon_info,
  prefix = 'amazon_info', key = 'id', con = con)
cached_amazon_info(c(10, 20), force. = TRUE) # Will forcibly repopulate.
```

## Advanced features

We can use multiple primary keys and salts.

```r
grab_sql_table <- function(table_name, year, month, dbname = 'default') {
  # Imagine we have some function that given a table name
  # and a database name returns a data.frame with aggregate
  # information about records created in that table from a
  # given year and month (e.g., ensuring each table has a
  # created_at column). This function will return a data.frame
  # with one record for each year-month pair, with at least
  # the columns "year" and "month".
}

cached_sql_table <- cachemeifyoucan::cache(grab_sql_table,
  key = c('year', 'month'), salt = c('table_name', 'dbname'), con = con,
  prefix = 'sql_table')
```

We would like to use a separate table to cache each combination of
table_name and dbname. Note that the character vector passed into
the `salt` parameter has to exactly match the names of the formal
arguments in the initial function, and must also be the name of
the columns returned by the data.frame. If these do not agree,
you can wrap your function. For example, if the data.frame returned
has 'mth' and 'yr' columns, you could instead cache the wrapper:

```r
wrap_sql_table <- function(table_name, yr, mth, dbname = 'default') {
  grab_sql_table(table_name = table_name, year = yr, month = mth, dbname = dbname)
}
```

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
