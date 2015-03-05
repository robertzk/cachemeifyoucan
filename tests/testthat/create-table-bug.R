## pre-bugfix : this blows up with the "Create Table" bug. 

go <- function () {
    fn <- function (n) data.frame(n = n, x = 1:n, y = runif(n))
    f2 <- cache(fn, key = 'n', salt = c(), con = "~/dev/cachemeifyoucan/tests/test-resources/database.yml", env = "test.local", prefix = "f2")

    f2(4)
}
