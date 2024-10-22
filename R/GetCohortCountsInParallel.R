#' @export
getCohortCountsInParallel <- function(cdmSources = NULL,
                                      cohortTableBaseName,
                                      cohortDefinitionSet = NULL,
                                      sequence = 1,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      databaseIds = NULL) {
  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)
  
  connectionDetails <- ParallelExecution::createConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  
  cohortCounts <- c()
  inclusion <- c()
  inclusionResult <- c()
  inclusionStats <- c()
  summaryStats <- c()
  censorStats <- c()
  
  for (i in (1:nrow(cdmSources))) {
    sourceKey <- cdmSources[i, ]$sourceKey
    cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = paste0(cohortTableBaseName, "_", cdmSources[i, ]$sourceKey))
    
    result <- CohortResults::getCohortInclusionRules(
      connection = connection,
      cohortDatabaseSchema = cdmSources[i, ]$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortIds = cohortDefinitionSet$cohortId
    ) |>
      dplyr::mutate(databaseId = sourceKey)
    
    cohortCounts[[i]] <- result$cohortCount
    inclusion[[i]] <- result$inclusion
    inclusionResult[[i]] <- result$inclusionResult
    inclusionStats[[i]] <- result$inclusionStats
    summaryStats[[i]] <- result$summaryStats
    censorStats[[i]] <- result$censorStats
  }
  
  results <- c()
  results$cohortCounts <- dplyr::bind_rows(cohortCounts)
  results$inclusion <- dplyr::bind_rows(inclusion)
  results$inclusionResult <- dplyr::bind_rows(inclusionResult)
  results$inclusionStats <- dplyr::bind_rows(inclusionStats)
  results$summaryStats <- dplyr::bind_rows(summaryStats)
  results$censorStats <- dplyr::bind_rows(censorStats)
  
  return(results)
}
