# Scrape Waze Data and Store json locally; use send_waze_zip_to_s3.R to zip multiple jsons
# together and send zip file to s3 bucket.

library(httr)
library(jsonlite)

CHECK_IF_HAS_DATA <- FALSE
CHECK_IF_HAS_NEW_DATA <- FALSE
WAZE_URL <- "https://world-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=World%20Bank&format=JSON&types=traffic,alerts,irregularities&polygon=33.556000,-1.193000;33.600000,0.125000;34.567000,1.839000;33.600000,4.515000;35.534000,5.391000;37.028000,4.603000;39.313000,3.946000;40.983000,4.734000;42.302000,4.121000;42.214000,3.375000;41.423000,0.872000;41.906000,-1.852000;40.939000,-3.301000;39.357000,-5.272000;38.215000,-4.703000;33.556000,-1.193000;33.556000,-1.193000"

SEND_INDIVIDUAL_JSONS_TO_S3 <- FALSE
# Only fill out below parameters if SEND_INDIVIDUAL_JSONS_TO_S3 is set to TRUE.
AWS_ACCESS_KEY_ID <- "XXXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
S3_BUCKET_NAME <- "XXXXXXXXXXX"
JSON_FILE_NAME_PREFIX <- "waze_" #prefix name for json file uploaded to s3 bucket

options(warn=-1)

# Scrape Data ------------------------------------------------------------------
url <- paste0(WAZE_URL)
req <- httr::GET(url)
json <- httr::content(req, as = "text")

# Checks if should store data --------------------------------------------------
# Check 1: Has data
# Check 2: Has new data

json_test <- jsonlite::fromJSON(json)
scrape_json <- TRUE

# CHECK 1: Check if have more than 4 names (ie, check if has data)
if(CHECK_IF_HAS_DATA){
  scrape_json <- (length(names(json_test)) > 4)
}

# CHECK 2: Check if different from pervious file [only if has data]
if(scrape_json & CHECK_IF_HAS_NEW_DATA & file.exists("waze_json_latest.json")){
  json_latest <- jsonlite::fromJSON("waze_json_latest.json")
  
  # Make time variables all the same
  json_latest$endTimeMillis <- 1
  json_latest$startTimeMillis <- 1
  json_latest$startTime <- 1
  json_latest$endTime <- 1
  
  json_test$endTimeMillis <- 1
  json_test$startTimeMillis <- 1
  json_test$startTime <- 1
  json_test$endTime <- 1
  
  # If they aren't identical, then upload file to s3
  scrape_json <- !identical(json_latest, json_test)
}

# Export json file locally -----------------------------------------------------
if(scrape_json){
  
  # Check if json_temporary_storage directory exists; if it doesn't exist, create the directory
  if(file.exists("json_temporary_storage") == FALSE) dir.create("json_temporary_storage")
  
  # Write latest JSON file
  write(json, "waze_json_latest.json")
  
  # Download json file to json_temporary_storage directory
  json_file_name <- paste0(JSON_FILE_NAME_PREFIX,gsub("-|:| ","_",Sys.time()),".json")
  write(json, file.path("json_temporary_storage", json_file_name))
}

quit(save="no")



