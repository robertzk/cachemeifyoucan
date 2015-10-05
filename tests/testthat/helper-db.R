database.yml <- system.file(package = "cachemeifyoucan", file.path("test_data", "database.yml"))

dbconn <- function() {
  conn <- try(silent = TRUE, db_connection(database.yml))
  if (is(conn, 'try-error')) {
    stop("Cannot run tests until you create a database named ", sQuote("travis"),
         "for user ", sQuote("postgres"), ". (You should be able to open ",
         "the PostgreSQL console using ", dQuote("psql postgres"),
         " from the command line. ",
         "From within there, run ", dQuote(paste0("CREATE DATABASE travis; ",
         "GRANT ALL PRIVILEGES ON DATABASE travis TO postgres;")),
         " You might also need to run ", dQuote("ALTER ROLE postgres WITH LOGIN;"),
         ")", call. = FALSE)
  }
  conn
}

