#' @export
dropCohortTablesInParallel <- function(cdmSources,
                                       sequence = 1,
                                       databaseIds = getListOfDatabaseIds(),
                                       cohortTableBaseName,
                                       connection = NULL,
                                       connectionDetails = createConnectionDetails(),
                                       dropCohortTable = FALSE) {
  cdmSources <-
    getCdmSource(
      cdmSources = cdmSources,
      database = databaseIds,
      sequence = sequence
    )

  if (nrow(cdmSources) == 0) {
    stop("no eligible cdm data source")
  }

  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  for (i in (1:nrow(cdmSources))) {
    cohortTableName <- paste0(
      cohortTableBaseName,
      "_",
      stringr::str_squish(cdmSources[i, ]$sourceKey)
    )
    cohortTableNames <-
      CohortGenerator::getCohortTableNames(cohortTable = cohortTableName)

    CohortGenerator::dropCohortStatsTables(
      connection = connection,
      cohortDatabaseSchema = cdmSources[i, ]$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = dropCohortTable
    )
  }
}
