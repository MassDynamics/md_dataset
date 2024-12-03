process <- function(intensity_dataframe, metadata_dataframe, message) {
  print(message)
  output <- rev(intensity_dataframe)
  return(list(output=output))
}
