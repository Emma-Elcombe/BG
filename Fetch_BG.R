
## BG survey Fetch

# packages
library(readxl)
library(devtools)
install_github(repo="DerekYves/rsurveygizmo")
library(futile.logger)
library(dplyr)


## there are 3 levels of logging in this file TRACE (most words), INFO, and ERROR (least words)
#  by default, script is logged at the INFO level
#  All ERROR messages are also reported to Error folder, see log_and_quit().

setwd("C:/R")
try(rm(err_msg))

## start log file
flog.appender(appender.file("Log/BG_fetch.log"), name='logger.c')
flog.info("START", name='logger.c')
flog.info("BG Fetch Started - Working Directory set", name='logger.c')


### set error handling.
log_and_quit <- function() {
  # Log error
  if(exists("err_msg")) {
    print('exists')
  } else {
    err_msg <- geterrmessage()
  }
  flog.error("Fatal Error: %s", err_msg, name='logger.c')
  setwd("C:/R/BatchInterfaces/BG")
  flog.appender(appender.file("BG_fetch.txt"), name='logger.a')
  flog.info("Failure", name='logger.a')
  setwd("//ad.uws.edu.au/dfshare/HomesCMB$/30042685/Desktop")
  flog.appender(appender.file("BG FETCH FAILED.log"), name='logger.b')
  flog.info("BG Fetch failed: %s", err_msg, name='logger.b')
  setwd("C:/R/BatchInterfaces/BG")
}
options(error = log_and_quit)

##### Wrap rest of script in a function #####
Extract_Load_Data <- function(){
  
  
  #### PART A - Load Reference Data

  #Load Sites file
  sites <- read_excel("Reference/sitesGizmo.xlsx", sheet = "sites")
  
  
  #### PART B - Fetch Data
  
  
  ### ROUND 1 - Jersey
  #STEP 1 - Set sites to fetch data from
  
  survey_name <-  "BG"
  
  set_fetch_sites <- function(survey_name, site_name){
    fetch_sites <- subset(sites, sites$survey == survey_name) 
    return(fetch_sites)
  }
  
  fetch_sites <- subset(sites, sites$survey == survey_name)
  
  ## STEP 2 - Fetch data
  
  # for each site...
  get_site_data <- function(sites_df,site_num) {
    # get the Survey ID
    survey_id <- sites_df[site_num,]$survey_id
    # download the data direct into dataframe
    flog.info("%s survey being fetched", survey_id, name='logger.c')
    this_site_df <-Rsurveygizmo::pullsg(survey_id, api="c46686a8c42e2972b07e59cbdc2f6e6e43932276e5e942953d", completes_only = TRUE, clean = TRUE)
    rows_fetched <- max(row(this_site_df))
    flog.info(" - %s rows added", rows_fetched, name='logger.c')
    # remove unnecessary columns 
    this_site_df <- this_site_df[1:9]
    # Re-name columns
    names(this_site_df) <- c("id", "status", "date_uploaded","date_start", "response_id", "UniqueID",
                             "Level", "date_surveyed","Interviewer")
    # add recoding and value-adding code here
    this_site_df$date_surveyed <- as.Date(substr(this_site_df$date_surveyed,1,10), "%d/%m/%Y")
    this_site_df$response_id <- paste(sites_df[site_num,]$survey_id,"-",this_site_df$response_id, sep="")
    this_site_df$survey <- paste(sites_df[site_num,]$site)
    this_site_df$id <- NULL
    this_site_df$date_start <- NULL
    # return the dataframe
    return(this_site_df)
  }
  
  # assemble sites data
  # first delete any existing sites data
  try(rm(fetch_sites))
  
  # now loop over all sites
  Load_site_data <- function(){
    rows_fetched <- max(row(subset(sites, sites$survey == survey_name ) ))
    flog.info("%s %s surveys selected", rows_fetched, survey_name, name='logger.c')
    for (i in seq_len(rows_fetched)) {
      fetch_sites <- set_fetch_sites(survey_name,sites$site)
      if (exists("fetched_data")) {
        fetched_data <- rbind(fetched_data, get_site_data(fetch_sites,i))
        flog.trace(" - rows added to sites_data", name='logger.c')
      } else {
        fetched_data <- get_site_data(fetch_sites,i)
        flog.trace(" - rows added to sites_data", name='logger.c')
      }
    } 
    rows_fetched <- max(row(fetched_data))
    flog.info("In total %s rows were extracted", rows_fetched, name='logger.c')
    return(fetched_data)
  }
  
  sites_data <- Load_site_data()
  
  
  ##STEP 3 - Clean downloaded data and format to match "Base" Data
  
  # reorder variables to match
  load_file <- sites_data[c(8,3,4,5,2,6,7,1)]
  load_file <- load_file %>% arrange(survey, desc(date_surveyed)) 
  flog.trace(" - columns re-ordered", name='logger.c')
  
 
  # check for duplicates
  load_file$duplicate <- paste(load_file$survey, load_file$UniqueID)
  duplicates <- sum(duplicated(load_file$duplicate))
  load_file$duplicate <- duplicated(load_file$duplicate)
  flog.trace("number of duplicates: %s", duplicates, name='logger.c')
  
  
  # add todays date
  extract_date <- as.character(Sys.Date())
  load_file <- data.frame(load_file[c(1:9)],extract_date)
  
  # Clean up
  flog.trace("clean up files", name='logger.c')
  rm(sites_data)
  try(rm(err_msg))
  
  
  #### Part D - export
  
  # export PEI data to load folder
  BG_file_name <- paste("Load/BG_Survey_Data.csv", sep = "")
  write.table(load_file, file = BG_file_name, sep=",", row.names = FALSE)
  flog.info("File %s saved into Load folder", BG_file_name, name='logger.c')
  
  # export PEI data to archive folder
  BG_file_name <- paste("Archive/BG_Data ", extract_date,".csv", sep = "")
  write.table(load_file, file = BG_file_name, sep=",", row.names = FALSE)
  flog.info("File %s saved into Archive folder", BG_file_name, name='logger.c')
  flog.info("END", BG_file_name, name='logger.c')
  
  setwd("C:/R/BatchInterfaces/BG")
  flog.appender(appender.file("BG_fetch.txt"), name='logger.a')
  flog.info("Success", name='logger.a')
  
}

tryCatch(Extract_Load_Data(), finally = log_and_quit())

