from google.cloud import bigquery

GA_SESSIONS_SCHEMA = [
    bigquery.SchemaField("fullVisitorId", "STRING"),
    bigquery.SchemaField("visitId", "INT64"),
    bigquery.SchemaField("visitStartTime", "INT64"),
    bigquery.SchemaField("visitNumber", "INT64"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("channelGrouping", "STRING"),

    bigquery.SchemaField(
        "device", "RECORD",
        fields=[
            bigquery.SchemaField("browser", "STRING"),
            bigquery.SchemaField("operatingSystem", "STRING"),
            bigquery.SchemaField("isMobile", "BOOL"),
        ]
    ),

    bigquery.SchemaField(
        "geoNetwork", "RECORD",
        fields=[
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("continent", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("subContinent", "STRING"),
            bigquery.SchemaField("cityId", "STRING"),
            bigquery.SchemaField("latitude", "STRING"),
            bigquery.SchemaField("longitude", "STRING"),
            bigquery.SchemaField("metro", "STRING"),
            bigquery.SchemaField("networkDomain", "STRING"),
            bigquery.SchemaField("networkLocation", "STRING"),
        ]
    ),

    bigquery.SchemaField(
        "totals", "RECORD",
        fields=[
            bigquery.SchemaField("visits", "INT64"),
            bigquery.SchemaField("hits", "INT64"),
            bigquery.SchemaField("bounces", "INT64"),
            bigquery.SchemaField("pageviews", "INT64"),
            bigquery.SchemaField("newVisits", "INT64"),
        ]
    ),

    bigquery.SchemaField(
        "trafficSource", "RECORD",
        fields=[
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"),
            bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("adContent", "STRING"),
            bigquery.SchemaField("referralPath", "STRING"),
        ]
    ),

    bigquery.SchemaField(
        "customDimensions", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("index", "INT64"),
            bigquery.SchemaField("value", "STRING"),
        ]
    ),

    bigquery.SchemaField(
        "hits_sample", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("hitNumber", "INT64"),
            bigquery.SchemaField("hostname", "STRING"),
            bigquery.SchemaField("isInteraction", "BOOL"),
            bigquery.SchemaField("pagePath", "STRING"),
            bigquery.SchemaField("pageTitle", "STRING"),
            bigquery.SchemaField("time", "INT64"),
            bigquery.SchemaField("type", "STRING"),
        ]
    ),

    bigquery.SchemaField("source_file", "STRING")
]


