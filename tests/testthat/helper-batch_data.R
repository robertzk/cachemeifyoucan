# This function gives deterministic output for each key.
batch_data <- function(key, model_version = "model_test", type = "record_id",
  switch = FALSE, flip = integer(0), add_column = FALSE) {
  original <- Reduce(rbind, lapply(key, function(key) {
    seed <- as.integer(paste0("0x", substr(digest(paste(key, model_version, type)), 1, 6)))
    set.seed(seed)
    data.frame(id = key, x = runif(1), y = rnorm(1), stringsAsFactors = FALSE)
  }))
  ret <- original
  if (switch) { ret$y <- NA }
  if (switch && length(flip) >= 1) {
    ret[flip, "y"] <- original[flip, "y"]
  }
  if (add_column) {
    ret$new_col <- 1
  }
  ret
}


batch_huge_data <- function(key, version = "version", type = "some_id") {
  ret <- data.frame(matrix(0, nrow = length(key), ncol = 1e4))
  ret$id <- key
  # Simulate slowness
  Sys.sleep(3)
  ret
}
