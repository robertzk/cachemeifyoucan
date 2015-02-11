database.yml <- system.file(package = "cachemeifyoucan", file.path("test_data", "database.yml"))

dbconn <- function() {
  conn <- try(silent = TRUE, db_connection(database.yml))
  if (is(conn, 'try-error')) {
    stop("Cannot run tests until you create a database named ", sQuote("travis"),
         "for user ", sQuote("postgres"), ".", call. = FALSE)
  }
  conn
}

