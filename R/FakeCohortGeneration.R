#' @export
fakeCohortGeneration <- function(cohortDefinitionSet, incrementalFolder) {
  cohortDefinitionSet <- cohortDefinitionSet |>
    dplyr::mutate(checksum = CohortGenerator:::computeChecksum(sql))
  
  recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
  
  if (file.exists(recordKeepingFile)) {
    recordKeeping <- CohortGenerator:::.readCsv(file = recordKeepingFile)
    recordKeeping <- recordKeeping |>
      dplyr::filter(!checksum %in% cohortDefinitionSet$checksum) |>
      dplyr::bind_rows(
        cohortDefinitionSet |>
          dplyr::select(cohortId, checksum) |>
          dplyr::mutate(timeStamp =  Sys.time())
      )
  }
  else {
    recordKeeping <-
      cohortDefinitionSet |>
      dplyr::select(cohortId, checksum) |>
      dplyr::mutate(timeStamp =  Sys.time())
    
  }
  CohortGenerator:::.writeCsv(x = recordKeeping, file = recordKeepingFile)
}