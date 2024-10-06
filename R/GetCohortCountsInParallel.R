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
  
  outputFolder <- tempfile()
  dir.create(outputFolder,
             showWarnings = FALSE,
             recursive = TRUE)
  
  cohortCounts <- c()
  inclusion <- c()
  inclusionResult <- c()
  inclusionStats <- c()
  summaryStats <- c()
  censorStats <- c()
  
  for (i in (1:nrow(cdmSources))) {
    sourceKey <- cdmSources[i, ]$sourceKey
    cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = paste0(cohortTableBaseName, "_", cdmSources[i, ]$sourceKey))
    
    cohortCounts[[i]] <- CohortGenerator::getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cdmSources[i, ]$cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      cohortIds = cohortDefinitionSet$cohortId
    ) |>
      dplyr::mutate(databaseId = sourceKey) |>
      dplyr::tibble()
    
    inclusion[[i]] <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@stat_table WHERE cohort_definition_id IN (@cohort_ids);",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cdmSources[i, ]$cohortDatabaseSchema,
      stat_table = cohortTableNames$cohortInclusionTable,
      cohort_ids = cohortDefinitionSet$cohortId
    ) |> dplyr::tibble()
    
    inclusionResult[[i]] <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@stat_table WHERE cohort_definition_id IN (@cohort_ids);",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cdmSources[i, ]$cohortDatabaseSchema,
      stat_table = cohortTableNames$cohortInclusionResultTable,
      cohort_ids = cohortDefinitionSet$cohortId
    ) |> dplyr::tibble()
    
    inclusionStats[[i]] <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@stat_table WHERE cohort_definition_id IN (@cohort_ids);",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cdmSources[i, ]$cohortDatabaseSchema,
      stat_table = cohortTableNames$cohortInclusionStatsTable,
      cohort_ids = cohortDefinitionSet$cohortId
    ) |> dplyr::tibble()
    
    summaryStats[[i]] <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@stat_table WHERE cohort_definition_id IN (@cohort_ids);",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cdmSources[i, ]$cohortDatabaseSchema,
      stat_table = cohortTableNames$cohortSummaryStatsTable,
      cohort_ids = cohortDefinitionSet$cohortId
    ) |> dplyr::tibble()
    
    censorStats[[i]] <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@stat_table WHERE cohort_definition_id IN (@cohort_ids);",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cdmSources[i, ]$cohortDatabaseSchema,
      stat_table = cohortTableNames$cohortCensorStatsTable,
      cohort_ids = cohortDefinitionSet$cohortId
    ) |> dplyr::tibble()
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
