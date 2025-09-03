# Legacy format - returns a named list with intensity and metadata
process_legacy <- function(intensity_dataframe, metadata_dataframe, message) {
  reversed <- rev(intensity_dataframe)

  more_output <- data.frame(
    Test = c("First"),
    Message = c(message)
  )

  return_object <- list(output=reversed, more_output=more_output)
  names(return_object) <- c("intensity", "metadata")

  return(return_object)
}

# New format - returns a structure that can be converted to IntensityData objects
process <- function(intensity_dataframe, metadata_dataframe, message) {
  reversed <- rev(intensity_dataframe)

  more_output <- data.frame(
    Test = c("First"),
    Message = c(message)
  )

  # Return a list where each element represents an IntensityData object
  # Each element should be a list with 'entity' and 'tables' components
  return(list(
    list(
      entity = "Protein",
      tables = list(
        list(type = "intensity", data = reversed),
        list(type = "metadata", data = more_output)
      )
    )
  ))
}
