getCohortDiagnosticsDefaultCovariateSettings <- function() {
  FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsIndexYearMonth = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useConditionOccurrence = TRUE,
    useProcedureOccurrence = TRUE,
    useDrugEraStart = TRUE,
    useMeasurement = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    useVisitCount = TRUE,
    useVisitConceptCount = TRUE,
    useConditionEraGroupStart = TRUE,
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = TRUE,
    useDrugEraGroupStart = TRUE,
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = TRUE,
    useDcsi = TRUE,
    useChads2 = TRUE,
    useChads2Vasc = TRUE,
    useHfrs = FALSE,
    temporalStartDays = c(
      # components displayed in cohort characterization
      -9999, # anytime prior
      -365, # long term prior
      -180, # medium term prior
      -30, # short term prior
      
      # components displayed in temporal characterization
      -365, # one year prior to -31
      -30, # 30 day prior not including day 0
      0, # index date only
      1, # 1 day after to day 30
      31,
      -9999 # Any time prior to any time future
    ),
    temporalEndDays = c(
      0, # anytime prior
      0, # long term prior
      0, # medium term prior
      0, # short term prior
      
      # components displayed in temporal characterization
      -31, # one year prior to -31
      -1, # 30 day prior not including day 0
      0, # index date only
      30, # 1 day after to day 30
      365,
      9999 # Any time prior to any time future
    )
  )
}
