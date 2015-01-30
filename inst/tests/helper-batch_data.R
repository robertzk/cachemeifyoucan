# This function gives deterministic output for each id.
batch_data <- function(id, model_version = "model_test", type = "record_id", 
  switch = FALSE, flip = integer(0), ...) {
  original <- Reduce(rbind, lapply(id, function(id) {
    seed <- as.integer(paste0("0x", substr(digest(paste(id, model_version, type)), 1, 6)))
    set.seed(seed)
    data.frame(id = id, x = runif(1), y = rnorm(1))}))
  ret <- original
  if (switch) ret$y <- NA
  if (switch && length(flip) >= 1)
    ret[flip, "y"] <- original[flip, "y"]
  ret
}
