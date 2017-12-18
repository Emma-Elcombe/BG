

## BG_Cohort

library(readxl)
library(foreign)
library(tidyr)
library(dplyr)
library(devtools)
install_github(repo="DerekYves/rsurveygizmo")
library(futile.logger)

# Get data
setwd("C:/R")
dataset = read.spss("C:/R/Reference/BG Cohort_20171212_EE.sav", to.data.frame=TRUE)
try(rm(err_msg))

## start log file
flog.appender(appender.file("Log/BG_fetch.log"), name='logger.c')
flog.info("SPSS Cohort fetch", name='logger.c')
flog.info("SPSS Cohort Fetch Started - Working Directory set", name='logger.c')

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
  flog.appender(appender.file("cohort_fetch.txt"), name='logger.a')
  flog.info("Failure", name='logger.a')
  setwd("//ad.uws.edu.au/dfshare/HomesCMB$/30042685/Desktop")
  flog.appender(appender.file("BG FETCH FAILED.log"), name='logger.b')
  flog.info("SPSS Cohort Fetch failed: %s", err_msg, name='logger.b')
  setwd("C:/R")
}
options(error = log_and_quit)

##### Wrap rest of script in a function #####
Extract_Load_Data <- function(){
  

# select subset of data
BG_cohort_ref <- dataset[c("Cohort","Unique_ID","Status","Child_DOB","DOV_3.5y","DOV_4y",
                           "DOA_4yPaed","DOV_4.5y","DOV_SchRead1","Yr_Sch_Start","DOV_EndKindy")]

# Format date fields
flog.info("Format data", name='logger.c')
spss2date <- function(x) as.Date(x/86400, origin = "1582-10-14")
BG_cohort_ref$Child_DOB <- spss2date(BG_cohort_ref$Child_DOB)
BG_cohort_ref$DOV_3.5y <- spss2date(BG_cohort_ref$DOV_3.5y)
BG_cohort_ref$DOV_4y <- spss2date(BG_cohort_ref$DOV_4y)
BG_cohort_ref$DOA_4yPaed <- spss2date(BG_cohort_ref$DOA_4yPaed)
BG_cohort_ref$DOV_4.5y <- spss2date(BG_cohort_ref$DOV_4.5y)
BG_cohort_ref$DOV_SchRead1 <- spss2date(BG_cohort_ref$DOV_SchRead1)
BG_cohort_ref$DOV_EndKindy <- spss2date(BG_cohort_ref$DOV_EndKindy)

# create tall file
BG_cohort_tall <- BG_cohort_ref[c("Cohort","Unique_ID","Status","Child_DOB","DOV_3.5y","DOV_4y",
                                  "DOA_4yPaed","DOV_4.5y","DOV_SchRead1","Yr_Sch_Start", "DOV_EndKindy")]
BG_cohort_tall <- BG_cohort_tall %>% filter(Status != "Not Recruited")
BG_cohort_tall$UniqueID <- as.character(BG_cohort_tall$Unique_ID)
BG_cohort_tall$UniqueID <- gsub(" ", "", BG_cohort_tall$UniqueID)
BG_cohort_tall$Unique_ID <- NULL
BG_cohort_tall$Yr_Sch_Start <- as.character(BG_cohort_tall$Yr_Sch_Start)
BG_cohort_tall$Yr_Sch_Start <- gsub(" ", "", BG_cohort_tall$Yr_Sch_Start)

BG_cohort_tall <- gather(BG_cohort_tall, key = "survey", value = "SPSS Date", DOV_3.5y, DOV_4y, DOA_4yPaed,
                         DOV_4.5y, DOV_SchRead1,DOV_EndKindy) 
BG_cohort_tall$survey <- gsub("DOV_", "", BG_cohort_tall$survey)
BG_cohort_tall$survey <- gsub("DOA_", "", BG_cohort_tall$survey)
BG_cohort_tall$survey <- gsub("y", "yr", BG_cohort_tall$survey)
BG_cohort_tall$survey <- gsub("SchRead1", "SchRead", BG_cohort_tall$survey)
BG_cohort_tall$survey <- gsub("Kindyr", "Kindy", BG_cohort_tall$survey)

#Load BG_survey_data file
load_file <- read.csv("C:/R/Load/BG_Survey_Data.csv", sep = ",")
flog.info("Load file created", name='logger.c')

# Merge with BG_survey Load_file

Gizmo_load_file <- load_file[c("UniqueID","date_surveyed","survey")] %>% 
  group_by(UniqueID, survey, date_surveyed) %>% summarise(n=n())
names(Gizmo_load_file)[3] <- "Gizmo Date"
Gizmo_load_file$survey <- gsub("SchoolReadiness", "SchRead", Gizmo_load_file$survey)
new <- BG_cohort_tall %>% left_join(Gizmo_load_file, by = c("UniqueID", "survey")) %>% select(-n)
flog.info("SPSS Cohort data merged with Gizmo data", name='logger.c')

# Determine due dates
new$child_age <- round(((Sys.Date() - new$Child_DOB)/365.25), digits = 2)
new$surv_age <- gsub("yr", "", new$survey)
new$surv_age <- gsub("Paed", "", new$surv_age)
new$surv_age <- gsub("SchRead", "", new$surv_age)
new$surv_age <- as.numeric(new$surv_age)

new$Date_dueP1 = new$Child_DOB + new$surv_age*365.25
new$Date_dueP2 <- as.Date(ifelse((is.na(new$Date_due) & new$survey == "SchRead" & new$Yr_Sch_Start == "2017"), "2017-02-01",
                            ifelse((is.na(new$Date_due) & new$survey == "EndKindy" & new$Yr_Sch_Start == "2017"), "2018-02-15",
                              ifelse((is.na(new$Date_due) & new$survey == "SchRead" & new$Yr_Sch_Start == "2018"), "2018-02-01",
                                ifelse((is.na(new$Date_due) & new$survey == "EndKindy" & new$Yr_Sch_Start == "2018"), "2019-02-15", "")))), "%Y-%m-%d")
new$Date_due = as.Date(ifelse(is.na(new$Date_dueP1), new$Date_dueP2 , new$Date_dueP1),origin = "1970-01-01")


# Determine survey status

new$surv_stat <- ifelse(!is.na(new$`Gizmo Date`),"Done (Gizmo)",
                   ifelse(!is.na(new$`SPSS Date`), "Done (SPSS)",
                     ifelse(new$Status == "Drop out","Inactive",
                       ifelse((!is.na(new$surv_age) & new$child_age > (new$surv_age+0.25)),"Missed",
                         ifelse((!is.na(new$surv_age) & new$child_age >= (new$surv_age-0.08)),"Due",
                           ifelse((!is.na(new$surv_age) & new$child_age < new$surv_age),"Not Due","Unknown"))))))

new$surv_stat <-ifelse((new$surv_stat == "Unknown" & new$Yr_Sch_Start == "2017" & new$survey == "SchRead") ,"Due",
                   ifelse((new$surv_stat == "Unknown" & new$Yr_Sch_Start == "2018" & new$survey == "SchRead"),"Not Due",
                     ifelse((new$surv_stat == "Unknown" & new$Yr_Sch_Start == "2017" & new$survey == "EndKindy"),"Due",
                       ifelse((new$surv_stat == "Unknown" & new$Yr_Sch_Start == "2018" & new$survey == "EndKindy"),"Not Due", new$surv_stat))))


BG_DOV <- new[c(1,2,5,4,6,7,8,9,10,14,13)]
flog.info("Due dates and status determined", name='logger.c')

# write Zoho BG_DOV table
write.table(BG_DOV, file = "Load/BG_DOV.csv", sep=",", na = "", row.names = FALSE)
flog.info("BG_DOV.csv saved to Load folder", name='logger.c')

setwd("C:/R/BatchInterfaces/BG")
flog.appender(appender.file("cohort_fetch.txt"), name='logger.a')
flog.info("Success", name='logger.a')

}

tryCatch(Extract_Load_Data(), finally = log_and_quit())
