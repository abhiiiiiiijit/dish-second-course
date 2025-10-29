CREATE TABLE IF NOT EXISTS analytics.ga_sessions (
  fullVisitorId STRING,
  visitId INT64,
  visitStartTime INT64,
  visitNumber INT64,
  date DATE,
  channelGrouping STRING,

  device STRUCT<
    browser STRING,
    operatingSystem STRING,
    isMobile BOOL
  >,

  geoNetwork STRUCT<
    city STRING,
    country STRING,
    continent STRING,
    region STRING,
    subContinent STRING,
    cityId STRING,
    latitude STRING,
    longitude STRING,
    metro STRING,
    networkDomain STRING,
    networkLocation STRING
  >,

  totals STRUCT<
    visits INT64,
    hits INT64,
    bounces INT64,
    pageviews INT64,
    newVisits INT64
  >,

  trafficSource STRUCT<
    source STRING,
    medium STRING,
    keyword STRING,
    adContent STRING,
    referralPath STRING
  >,

  customDimensions ARRAY<STRUCT<
    index INT64,
    value STRING
  >>,

  hits_sample ARRAY<STRUCT<
    hitNumber INT64,
    hostname STRING,
    isInteraction BOOL,
    pagePath STRING,
    pageTitle STRING,
    time INT64,
    type STRING
  >>,
  load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  loaded_by STRING DEFAULT SESSION_USER(),
  source_file STRING
)
PARTITION BY date
CLUSTER BY fullVisitorId, visitId;
