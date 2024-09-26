#' Execute the cohort generation
#'
#' @details
#' This function executes the cohort generation
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema name where intermediate data can be stored. You
#'                                            will need to have write privileges in this schema. Note
#'                                            that for SQL Server, this should include both the
#'                                            database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTableNames                    cohortTableNames
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param createCohortTableIncremental         When set to TRUE, this function will check to see if the
#'                                            cohortTableNames exists in the cohortDatabaseSchema and if
#'                                            they exist, it will skip creating the tables.
#' @param generateCohortIncremental           Create only cohorts that haven't been created before?
#' @param incrementalFolder                   Name of local folder to hold the logs for incremental
#'                                            run; make sure to use forward slashes (/). Do not use a
#'                                            folder on a network drive since this greatly impacts
#'                                            performance.
#' @param cohortDefinitionSet                 Cohort Definition set object
#' @param databaseId                          database id
#' @param cohortIds                           Do you want to limit the execution to only some cohort ids.
#'
#' @export
executeCohortGeneration <- function(connectionDetails,
                                    cohortDefinitionSet,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema = cdmDatabaseSchema,
                                    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    outputFolder,
                                    databaseId,
                                    minCellCount = 5,
                                    createCohortTableIncremental = TRUE,
                                    generateCohortIncremental = TRUE,
                                    incrementalFolder = file.path(outputFolder, "incrementalFolder"),
                                    cohortIds = NULL) {
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )
  
  ParallelLogger::logInfo("Creating cohorts")
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  
  DatabaseConnector::dropEmulatedTempTables(connection = connection)
  
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = createCohortTableIncremental
  )
  
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet |>
      dplyr::filter(.data$cohortId %in% c(cohortIds))
  }
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    tempEmulationSchema = tempEmulationSchema,
    incrementalFolder = incrementalFolder,
    stopOnError = FALSE,
    incremental = generateCohortIncremental
  )
  browser()
  
  dir.create(
    file.path(outputFolder, "CohortStatistics"),
    showWarnings = FALSE,
    recursive = TRUE
  )
  # export stats table to local
  CohortGenerator::exportCohortStatsTables(
    connectionDetails = NULL,
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = file.path(outputFolder, "CohortStatistics"),
    incremental = generateCohortIncremental,
    cohortDefinitionSet = cohortDefinitionSet,
    snakeCaseToCamelCase = TRUE,
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  output <- c()
  if (!is.null(cohortTableNames$cohortInclusionTable)) {
    output$cohortInclusionTable <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@table_name;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = cohortTableNames$cohortInclusionTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) |>
      dplyr::tibble() |>
      dplyr::filter(cohortDefinitionId %in% cohortDefinitionSet$cohortId) |> 
      dplyr::mutate(databaseId == !!databaseId)
  }
  
  if (!is.null(cohortTableNames$cohortInclusionResultTable)) {
    output$cohortInclusionResultTable <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@table_name;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = cohortTableNames$cohortInclusionResultTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) |>
      dplyr::tibble() |>
      dplyr::filter(cohortDefinitionId %in% cohortDefinitionSet$cohortId) |> 
      dplyr::mutate(databaseId == !!databaseId)
  }
  
  if (!is.null(cohortTableNames$cohortInclusionStatsTable)) {
    output$cohortInclusionStatsTable <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@table_name;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = cohortTableNames$cohortInclusionStatsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) |>
      dplyr::tibble() |>
      dplyr::filter(cohortDefinitionId %in% cohortDefinitionSet$cohortId) |> 
      dplyr::mutate(databaseId == !!databaseId)
  }
  
  if (!is.null(cohortTableNames$cohortSummaryStatsTable)) {
    output$cohortSummaryStatsTable <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@table_name;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = cohortTableNames$cohortSummaryStatsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) |>
      dplyr::tibble() |>
      dplyr::filter(cohortDefinitionId %in% cohortDefinitionSet$cohortId) |> 
      dplyr::mutate(databaseId == !!databaseId)
  }
  
  if (!is.null(cohortTableNames$cohortCensorStatsTable)) {
    output$cohortCensorStatsTable <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @cohort_database_schema.@table_name;",
      cohort_database_schema = cohortDatabaseSchema,
      table_name = cohortTableNames$cohortCensorStatsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) |>
      dplyr::tibble() |>
      dplyr::filter(cohortDefinitionId %in% cohortDefinitionSet$cohortId) |> 
      dplyr::mutate(databaseId == !!databaseId)
  }
  
  output$cohortCount <- CohortGenerator::getCohortCounts(
    connectionDetails = NULL,
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = databaseId,
    cohortIds = cohortDefinitionSet$cohortId
  ) |>
    dplyr::tibble() |>
    dplyr::select(databaseId,
                  cohortId,
                  cohortName,
                  cohortEntries,
                  cohortSubjects)
  
  DatabaseConnector::dropEmulatedTempTables(connection = connection)
  
  DatabaseConnector::disconnect(connection)
  
  readr::write_excel_csv(
    x = output$cohortCount |>
      dplyr::select(.data$cohortId, .data$cohortEntries, .data$cohortSubjects) |>
      dplyr::arrange(.data$cohortId),
    file = file.path(outputFolder, "CohortStatistics", "cohortCount.csv"),
    na = "",
    append = FALSE,
    progress = FALSE
  )
  return(output)
}



#' @export
executeCohortGenerationInParallel <- function(cdmSources,
                                              outputFolder,
                                              cohortDefinitionSet,
                                              cohortTableBaseName = "cohort",
                                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                              databaseIds = getListOfDatabaseIds(),
                                              sequence = 1,
                                              createCohortTableIncremental = TRUE,
                                              generateCohortIncremental = TRUE,
                                              cohortIds = NULL,
                                              minCellCount = 5) {
  dir.create(path = outputFolder,
             showWarnings = FALSE,
             recursive = TRUE)
  
  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)
  
  x <- list()
  for (i in 1:nrow(cdmSources)) {
    x[[i]] <- cdmSources[i, ]
  }
  
  # use Parallel Logger to run in parallel
  cluster <-
    ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
      parallel::detectCores() /
        2
    )), length(x)))
  
  ## file logger
  loggerName <-
    paste0(
      "CG_",
      stringr::str_replace_all(
        string = Sys.time(),
        pattern = ":|-|EDT| ",
        replacement = ""
      )
    )
  
  ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
  
  executeCohortGenerationX <- function(x,
                                       cohortDefinitionSet,
                                       cohortIds,
                                       cohortTableBaseName,
                                       outputFolder,
                                       tempEmulationSchema,
                                       createCohortTableIncremental,
                                       generateCohortIncremental,
                                       minCellCount) {
    connectionDetails <- createConnectionDetails()
    
    cohortTableName <- paste0(cohortTableBaseName,
                              "_",
                              stringr::str_squish(x$sourceKey))
    cohortTableNames <-
      CohortGenerator::getCohortTableNames(cohortTable = cohortTableName)
    
    executeCohortGeneration(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = cohortDefinitionSet,
      cdmDatabaseSchema = x$cdmDatabaseSchema,
      cohortDatabaseSchema = x$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      databaseId = x$sourceKey,
      tempEmulationSchema = tempEmulationSchema,
      outputFolder = file.path(outputFolder, x$sourceKey),
      cohortIds = cohortIds,
      createCohortTableIncremental = createCohortTableIncremental,
      generateCohortIncremental = generateCohortIncremental,
      minCellCount = minCellCount
    )
  }
  
  ParallelLogger::clusterApply(
    cluster = cluster,
    x = x,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortIds = cohortIds,
    outputFolder = outputFolder,
    cohortTableBaseName = cohortTableBaseName,
    tempEmulationSchema = tempEmulationSchema,
    createCohortTableIncremental = createCohortTableIncremental,
    generateCohortIncremental = generateCohortIncremental,
    minCellCount = minCellCount,
    fun = executeCohortGenerationX
  )
  
  ParallelLogger::stopCluster(cluster = cluster)
  ParallelLogger::clearLoggers()
}
