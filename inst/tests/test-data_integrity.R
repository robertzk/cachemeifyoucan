context('data integrity')

describe("cache function", {
  dbconn <- DBI::dbConnect(dbDriver("PostgreSQL"), "robk")

  test_that('it can expand the columns in a cached table', {
    
  })

  RPostgreSQL::postgresqlCloseConnection(dbconn)
})
