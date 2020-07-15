# This script combines individual json files scraped from waze stored in an ec2
# instance, merges them into one json file, sends that json file to an s3 bucket, 
# then deletes the individual json files in the ec2 instance that were merged

# Setup ------------------------------------------------------------------------
library(httr)
library(jsonlite)
library(aws.s3)
library(dplyr)
library(zip)

# PARAMETERS
AWS_ACCESS_KEY_ID <- "XXXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
S3_BUCKET_NAME <- "XXXXXXXXXXX"
ZIP_FILE_NAME_PREFIX <- "waze_" #prefix name for zip file uploaded to s3 bucket

options(warn=-1)

# Set AWS Key ------------------------------------------------------------------
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY)

# Function for Moving Files from One Directory to Another ----------------------
# https://stackoverflow.com/questions/10266963/moving-files-between-folders
my.file.rename <- function(from, to) {
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  file.rename(from = from,  to = to)
}

# Merge and Upload jsons -------------------------------------------------------
# Create vector of json names in json_temporary_storage directory
json_file_names <- list.files("json_temporary_storage", full.names=F)

# Only run following lines of code if files exist in directory
# Length should be greater than 1 to ensure all jsons are in list format
if(length(json_file_names) > 1){
  
  # Create Directory
  new_dir <- paste0(ZIP_FILE_NAME_PREFIX, gsub("-|:| ","_",Sys.time()))
  dir.create(new_dir)
  
  # Move Files Into Directory
  for(json_file_name in json_file_names){
    my.file.rename(from = file.path("json_temporary_storage",json_file_name), to = file.path(new_dir,json_file_name))
  }
  
  # Zip Directory
  zip(files=new_dir,zipfile=paste0(new_dir,".zip"))

  # Send zip file to s3 bucket  
  put_object(file = paste0(new_dir,".zip"), 
             object = paste0(new_dir,".zip"), 
             bucket = S3_BUCKET_NAME)
  
  # Delete json files that sent to s3 bucket
  unlink(new_dir, recursive=T)
  file.remove(paste0(new_dir,".zip"))
}

quit(save="no")
