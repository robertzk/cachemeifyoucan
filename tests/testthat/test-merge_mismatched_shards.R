context("merging mismatched shards")

describe("merge2 helper", {
  it("should correctly impute shards missing rows with NAs", {
    df  <- data.frame(a = c(1,2,3), b = c(3,4,5))
    df2 <- data.frame(a = c(2,3), c = c(4,5)) 
    df3 <- data.frame(a = c(1,2,3), b = c(3,4,5), c = c(NA, 4, 5))
    expect_identical(merge2(list(df2, df), "a"), df3)
    expect_identical(merge2(list(df, df2), "a"), df3)
  })
})

