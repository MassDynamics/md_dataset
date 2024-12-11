process <- function(intensity_dataframe, metadata_dataframe, message) {
  reversed <- rev(intensity_dataframe)

  more_output <- data.frame(
    Test = c("First"),
    Message = c(message)
  )
  return(list("output"=reversed, "more_output"=more_output))
}
