# Appends json files stored in s3 bucket into one

# Setup ------------------------------------------------------------------------
library(httr)
library(jsonlite)
library(aws.s3)
library(plyr)
library(dplyr)
library(rgdal)
library(raster)
library(pbmcapply)

# Define Functions -------------------------------------------------------------

# Read waze json or zipped jsons from s3 bucket
read_waze_json_or_zipped_s3 <- function(s3_key){
  
  print(paste0("Downloading: ", s3_key))
  
  if(endsWith(s3_key, ".zip")){
    out <- read_waze_jsons_zipped_s3(s3_key)
  }
  
  if(endsWith(s3_key, ".json")){
    out <- read_waze_json_s3(s3_key)
  }
  
  return(out)
}

# Read waze json data stored in zipped folder in s3 bucket
read_waze_jsons_zipped_s3 <- function(s3_key){
  object <- paste0("s3://",s3_key)
  save_object(object=object,file="temp.zip")
  unzip("temp.zip")
  files <- unzip("temp.zip",list=T)
  
  file_names <- files$Name
  file_names <- file_names[!endsWith(file_names, "/")] # remove keys that reference a folder (not a file within a folder)
  
  out_list <- lapply(file_names, function(file) process_waze_json(jsonlite::fromJSON(file)))
  
  #### Alerts
  alerts_df <- lapply(1:length(out_list), function(i) jsonlite::flatten(out_list[[i]]$alerts_df)) %>% bind_rows
  
  #### Jams
  jams_dataframe <- lapply(1:length(out_list), function(i) as.data.frame(out_list[[i]]$jams_sdf)) %>% bind_rows
  
  if(nrow(jams_dataframe) > 0){
  
  # List of geometries; if NULL, remove item from list
  jams_sdf <- lapply(1:length(out_list), function(i){
    if(nrow(out_list[[i]]$jams_sdf) == 0){
      return(NULL)
    } else{
      return(extract_jams_geometry(out_list[[i]]$jams_sdf))
    }
  }) %>% 
    unlist %>% # unlist removes NULL items in list
    do.call(what="rbind")
  jams_sdf@data <- jams_dataframe
  crs(jams_sdf) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
  } else{
    jams_sdf <- NULL
  }
  #jams_sdf <- lapply(1:length(out_list), function(i) out_list[[i]]$jams_sdf) %>% do.call(what="rbind")
  
  # Remove temporary folders
  unlink(strsplit(file_names[1],"/")[[1]][1], recursive=T)
  file.remove("temp.zip")
  
  return(list(alerts_df=alerts_df,
              jams_sdf=jams_sdf))
}

# Read waze json stored in s3 bucket
read_waze_json_s3 <- function(s3_key){
  object <- paste0("s3://",s3_key)
  
  json <- get_object(object=object) %>% rawToChar  %>% fromJSON
  
  out <- process_waze_json(json)
  
  return(out)
}

# Convert waze json to dataframe (alerts) and spatial dataframe (jams)
process_waze_json <- function(json){
  # Alerts
  if(is.null(json$alerts)){
    alerts_df <- matrix(nrow=1,ncol=0) %>% as.data.frame
  } else{
    alerts_df <- json$alerts
  }
  alerts_df$startTime <- json$startTime
  
  # Jams
  if(is.null(json$jams)){
    jams_sdf <- as.data.frame(NULL)
  } else{
    jams_df <- json$jams
    jam_data <- subset(jams_df, select=-c(line,segments))
    jam_line_list <- jams_df$line
    
    # Make sure x and y are in correct order (occassionally are flipped in json file)
    jam_line_list <- lapply(1:nrow(jam_data), function(i) cbind(jam_line_list[[i]]$x,jam_line_list[[i]]$y))
    
    line_list <- lapply(1:length(jams_df$line), function(i) Line(jam_line_list[[i]]))
    lines_list <- lapply(1:length(line_list), function(i) Lines(list(line_list[[i]]), ID=i))
    
    jams_sdf <- SpatialLines(lines_list)
    jams_sdf$temp_id <- 1:length(jams_sdf)
    jams_sdf@data <- jam_data
    
    jams_sdf$startTime <- json$startTime
  }
  
  return(list(alerts_df=alerts_df,
              jams_sdf=jams_sdf))
}

# Extracts geometry from jams dataframe. Used to append multiple spatial
# polygon dataframes together
extract_jams_geometry <- function(jams_sdf){
  jams_sdf$temp_id <- 1:nrow(jams_sdf)
  jams_sdf@data <- subset(jams_sdf@data, select = c(temp_id))
  return(jams_sdf)
}

# Get s3 Key from Contents
get_s3_keys <- function(i, s3_files) s3_files[i]$Contents$Key

# Extract last n characters from string
# https://stackoverflow.com/questions/7963898/extracting-the-last-n-characters-from-a-string-in-r
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

# Main Function ----------------------------------------------------------------

# Read waze data from s3 bucket; combines into single alerts dataframe and 
# single jams spatial dataframe
read_waze_s3_monthy <- function(aws_access_key_id=NULL,
                         aws_secret_access_key=NULL,
                         s3_bucket=NULL,
                         start_yyyy_mm_dd_hh_mm=NULL,
                         end_yyyy_mm_dd_hh_mm=NULL,
                         yyyy_mm = NULL,
                         mc_cores=1){
  
  #### Set AWS Kets
  if(!is.null(aws_access_key_id) & !is.null(aws_secret_access_key)){
    Sys.setenv("AWS_ACCESS_KEY_ID" = aws_access_key_id,
               "AWS_SECRET_ACCESS_KEY" = aws_secret_access_key)
  }
  
  #### Check for s3_bucket
  if(is.null(s3_bucket)) stop("Provide an s3 bucket name")
  
  #### Get s3 keys
  # Separate bucket and folders path within bucket
  s3_bucket_parts <- strsplit(s3_bucket,"/")[[1]]
  if(length(s3_bucket_parts) == 1){
    s3_bucket_name <- s3_bucket_parts
    s3_bucket_path <- NULL
  } else{
    s3_bucket_name <- s3_bucket_parts[1]
    s3_bucket_path <- s3_bucket_parts[2:length(s3_bucket_parts)] %>% paste(collapse="/")
    s3_bucket_path <- paste0(s3_bucket_path,"/")
  }
  
  # Get Keys
  s3_files <- get_bucket(bucket=s3_bucket_name, max=Inf, url_style="path", prefix=paste0(s3_bucket_path, yyyy_mm))
  s3_keys <- lapply(1:length(s3_files), get_s3_keys, s3_files) %>% unlist
  s3_keys <- s3_keys[!endsWith(s3_keys, "/")] # remove keys that reference a folder (not a file within a folder)
  
  # Add bucket name to s3_keys
  s3_keys <- paste0(s3_bucket_name, "/", s3_keys)
  
  #### Subset keys to daterange
  if(!is.null(start_yyyy_mm_dd_hh_mm) & !is.null(end_yyyy_mm_dd_hh_mm)){
    
    s3_key_datetimes <- gsub(".zip|.json","", s3_keys) %>% 
      substrRight(19) %>% 
      str_replace_all(".*/", "") %>%
      gsub(pattern="_",replacement="-")
    
    start_yyyy_mm_dd_hh_mm <- paste0(start_yyyy_mm_dd_hh_mm,"_00") %>% 
      gsub(pattern="_",replacement="-")
    end_yyyy_mm_dd_hh_mm <- paste0(end_yyyy_mm_dd_hh_mm,"_00") %>% 
      gsub(pattern="_",replacement="-")
    
    # Order s3 keys by datetime
    s3_keys <- s3_keys[order(s3_key_datetimes)]
    s3_key_datetimes <- s3_key_datetimes[order(s3_key_datetimes)]
    
    s3_keys_include <- (s3_key_datetimes >= start_yyyy_mm_dd_hh_mm) & (s3_key_datetimes <= end_yyyy_mm_dd_hh_mm)
    s3_keys_include_min_index <- min(which(s3_keys_include)) - 1
    s3_keys_include_max_index <- max(which(s3_keys_include)) + 1
    
    s3_keys <- s3_keys[s3_keys_include_min_index:s3_keys_include_max_index]
    s3_keys <- s3_keys[!is.na(s3_keys)]
  }
  
  #### Extract waze data as list of dataframes
  if(mc_cores > 1){
    waze_output_list <- pbmclapply(s3_keys, read_waze_json_or_zipped_s3, mc.cores=mc_cores)
  }else{
    waze_output_list <- lapply(s3_keys, read_waze_json_or_zipped_s3)
  }
  
  #### Append waze files into single alerts dataframe and single jams spatial dataframe
  ## Alerts
  alerts_df <- lapply(1:length(waze_output_list), function(i) jsonlite::flatten(waze_output_list[[i]]$alerts_df)) %>% bind_rows
  
  ## Jams
  jams_dataframe <- lapply(1:length(waze_output_list), function(i) as.data.frame(waze_output_list[[i]]$jams_sdf)) %>% bind_rows
  
  # List of geometries; if NULL, remove item from list
  jams_sdf <- lapply(1:length(waze_output_list), function(i){
    if(is.null(waze_output_list[[i]]$jams_sdf)){
      return(NULL)
    } else{
      return(extract_jams_geometry(waze_output_list[[i]]$jams_sdf))
    }
  }) %>% 
    unlist %>% # unlist removes NULL items in list
    do.call(what="rbind")
  jams_sdf@data <- jams_dataframe
  crs(jams_sdf) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
  
  # TODO: Restrict by daterange here again, for both alerts_df and jams_sdf
  
  return(list(alerts_df=alerts_df,
              jams_sdf=jams_sdf))
}

