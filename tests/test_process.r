process <- function(intensity_dataframe, metadata_dataframe, message) {
  reversed <- rev(intensity_dataframe)

  more_output <- data.frame(
    Test = c("First"),
    Message = c(message)
  )

  return_object <- (list(output=reversed, more_output=more_output))
  names(return_object) <- c("Protein_Intensity", "Protein_Metadata")

  return(return_object)
}
