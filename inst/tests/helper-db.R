database.yml <- system.file(package = "cachemeifyoucan", file.path("test_data", "database.yml"))

dbconn <- function() { db_connection(database.yml) }

