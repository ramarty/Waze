# Scrape Waze Data and Send Individual json to s3 Bucket

library(httr)
library(jsonlite)

CHECK_IF_HAS_DATA <- FALSE
CHECK_IF_HAS_NEW_DATA <- FALSE
WAZE_URL <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

AWS_ACCESS_KEY_ID <- "XXXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
S3_BUCKET_NAME <- "XXXXXXXXXXX"
JSON_FILE_NAME_PREFIX <- "waze_" #prefix name for json file uploaded to s3 bucket

options(warn=-1)

# Keys -------------------------------------------------------------------------
# Set AWS Key
Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
           "AWS_SECRET_ACCESS_KEY" = "XXXXXXXXXXXXXXX")
S3_BUCKET_NAME <- "XXXXXXXXXXXXXXX"

Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY)

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
  
  # Write latest JSON file
  write(json, "waze_json_latest.json")
  
  put_object(file = "waze_json_latest.json", 
             object = paste0(JSON_FILE_NAME_PREFIX,gsub("-|:| ","_",Sys.time()),".json"),
             bucket = S3_BUCKET_NAME)
}

quit(save="no")



