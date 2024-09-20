#' Create Connection Details for Databricks
#'
#' This function creates connection details for a Databricks database using the DatabaseConnector package.
#' It retrieves the connection string, username, and password from a specified keyring.
#'
#' @param keyringName A character string specifying the name of the keyring. Defaults to the value of the environment variable \code{keyringName}.
#' @return A list containing the connection details.
#' @export
createConnectionDetails <- function(keyringName = Sys.getenv("keyringName")) {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    connectionString = keyring::key_get("dataBricksConnectionString", keyring = keyringName),
    user = keyring::key_get("dataBricksUserName", keyring = keyringName),
    password = keyring::key_get("dataBricksPassword", keyring = keyringName)
  )

  return(connectionDetails)
}
