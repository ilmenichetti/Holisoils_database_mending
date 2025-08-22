library(tidyr)
library(dplyr)
library(arrow)

#set the working dir where this script is located (just in case)
library(here)
setwd(here::here())

# Gas ID mapping: 1=co2, 2=ch4, 3=n2o

# Load and merge databases
db1_data <- read.csv("../holisoils-old-data/co2ch4n2o/db_data.csv")
db1_meas <- read.csv("../holisoils-old-data/co2ch4n2o/db_meas.csv")
db1_series <- read.csv("../holisoils-old-data/co2ch4n2o/db_series.csv")
db1_flux <- read.csv("../holisoils-old-data/co2ch4n2o/db_flux.csv")

db2_data <- read.csv("../holisoils-old-data/ghgdata/db_data.csv")
db2_meas <- read.csv("../holisoils-old-data/ghgdata/db_meas.csv")
db2_series <- read.csv("../holisoils-old-data/ghgdata/db_series.csv")
db2_flux <- read.csv("../holisoils-old-data/ghgdata/db_flux.csv")

# Add database origin identifiers
db1_data$db_origin <- 1
db1_meas$db_origin <- 1
db1_series$db_origin <- 1
db1_flux$db_origin <- 1

db2_data$db_origin <- 2
db2_meas$db_origin <- 2
db2_series$db_origin <- 2
db2_flux$db_origin <- 2

# Clean site IDs
db1_series$siteid[db1_series$siteid == "Karstula_75"] <- "Karstula75"
db1_series$siteid[db1_series$siteid == "Karstula_76"] <- "Karstula76"
db2_series$siteid[db2_series$siteid == "Karstula_75"] <- "Karstula75"
db2_series$siteid[db2_series$siteid == "Karstula_76"] <- "Karstula76"

db1_meas$siteids[db1_meas$siteids == "Karstula_75"] <- "Karstula75"
db1_meas$siteids[db1_meas$siteids == "Karstula_76"] <- "Karstula76"
db2_meas$siteids[db2_meas$siteids == "Karstula_75"] <- "Karstula75"
db2_meas$siteids[db2_meas$siteids == "Karstula_76"] <- "Karstula76"

# Merge databases
common_cols_meas <- intersect(names(db1_meas), names(db2_meas))
common_cols_series <- intersect(names(db1_series), names(db2_series))

db_series <- rbind(db1_series[,common_cols_series], db2_series[,common_cols_series])
db_meas <- rbind(db1_meas[,common_cols_meas], db2_meas[,common_cols_meas])
db_data <- rbind(db1_data, db2_data)
db_flux <- rbind(db1_flux, db2_flux)

# Clean up series data
db_series$unique_id <- seq_len(nrow(db_series))
columns_to_drop <- c("t15", "t20", "t30", "t40")
db_series <- db_series[, setdiff(names(db_series), columns_to_drop)]

# Create main folder structure
main_folder <- "Holisoils_GHG_data"
#Site IDS, include sites from both databases
unique(db1_meas$siteids)
unique(db2_meas$siteids)

site_ids <- unique(c(db1_meas$siteids, db2_meas$siteids))

#just exclude a few measurements hard to integrate
site_ids <- site_ids[!site_ids == "Karstula_75 Karstula_76"]


# Create site folders
for (folder in site_ids) {
  folder_path <- file.path(main_folder, folder)
  if (!dir.exists(folder_path)) {
    dir.create(folder_path, recursive = TRUE)
  }
}

# Initialize error tracking
errors_df <- data.frame(
  CO2_value = logical(), no_CO2_value = logical(), many_CO2_value = logical(), 
  CH4_value = logical(), no_CH4_value = logical(), many_CH4_value = logical(),
  error = character(), fluxID = numeric(), meas_set = character(), 
  dborigin = numeric(), folder = character(), stringsAsFactors = FALSE
)

# Main processing loop
for (folder in site_ids) {
  cat("Processing site:", folder, "\n")
  
  sub_path <- file.path(main_folder, folder)
  series_subset <- db_series[db_series$siteid == folder, ]
  
  if (nrow(series_subset) == 0) {
    cat("No series found for site:", folder, "\n")
    next
  }
  
  # Create campaign subfolders
  campaigns_df <- series_subset %>%
    distinct(measurements, db_origin) %>%
    mutate(subfolder = paste0(measurements, "_db", db_origin))
  
  for (j in seq_len(nrow(campaigns_df))) {
    subfolder_path <- file.path(sub_path, campaigns_df$subfolder[j])
    if (!dir.exists(subfolder_path)) {
      dir.create(subfolder_path, recursive = TRUE)
    }
  }
  
  # Initialize additional columns
  series_subset$file_id <- NA
  series_subset$subfolder <- NA
  series_subset$device <- NA
  series_subset$chamber <- NA
  series_subset$respiration <- NA
  series_subset$methane <- NA
  series_subset$sample_name <- series_subset$point
  series_subset$filename <- NA
  series_subset$autotrimmed <- FALSE  # Track autotrim status
  
  # Process each series
  for (i in seq_len(nrow(series_subset))) {
    series_id <- series_subset$id[i]
    subfolder <- paste0(series_subset$measurements[i], "_db", series_subset$db_origin[i])
    subfolder_path <- file.path(sub_path, subfolder)
    
    # Create filename
    filename <- paste(
      series_subset$date[i],
      series_subset$subsiteid[i],
      series_subset$point[i],
      series_subset$pointtype[i], 
      sep = "_"
    )
    
    # Write data file if measurements exist
    file_to_write <- db_data[db_data$series == series_id, ]
    if (nrow(file_to_write) > 0) {
      write_feather(file_to_write, file.path(subfolder_path, paste0(filename, ".feather")))
    } else {
      filename <- "not_found"
    }
    
    # Get measurement info
    meas_info <- db_meas[db_meas$id == series_subset$measurements[i] & 
                           db_meas$db_origin == series_subset$db_origin[i], ]
    
    series_subset$file_id[i] <- filename
    series_subset$device[i] <- if(nrow(meas_info) > 0) meas_info$device[1] else NA
    series_subset$chamber[i] <- if(nrow(meas_info) > 0) meas_info$chamber[1] else NA
    series_subset$subfolder[i] <- subfolder
    series_subset$filename[i] <- filename
    
    # Process CO2 flux (gas == 1)
    co2_fluxes <- db_flux[db_flux$gas == 1 & 
                            db_flux$series == series_id & 
                            db_flux$db_origin == series_subset$db_origin[i], ]
    
    error_base <- list(
      CO2_value = FALSE, no_CO2_value = FALSE, many_CO2_value = FALSE,
      CH4_value = FALSE, no_CH4_value = FALSE, many_CH4_value = FALSE,
      fluxID = series_id, meas_set = subfolder, 
      dborigin = series_subset$db_origin[i], folder = folder
    )
    
    if (nrow(co2_fluxes) == 1) {
      series_subset$respiration[i] <- co2_fluxes$flux[1]
      series_subset$autotrimmed[i] <- co2_fluxes$trimmer[1] == 1
      error_base$CO2_value <- TRUE
      error_base$error <- paste("CO2 flux found, series", series_id)
    } else if (nrow(co2_fluxes) == 0) {
      series_subset$respiration[i] <- NA
      error_base$no_CO2_value <- TRUE
      error_base$error <- paste("No CO2 flux found for series", series_id)
    } else {
      # FIX: Handle multiple flux values
      series_subset$respiration[i] <- co2_fluxes$flux[1]  # Take first
      series_subset$autotrimmed[i] <- co2_fluxes$trimmer[1] == 1
      error_base$many_CO2_value <- TRUE
      error_base$error <- paste("Multiple CO2 fluxes found for series", series_id, "- using first")
    }
    
    # Process CH4 flux (gas == 2)
    ch4_fluxes <- db_flux[db_flux$gas == 2 & 
                            db_flux$series == series_id & 
                            db_flux$db_origin == series_subset$db_origin[i], ]
    
    if (nrow(ch4_fluxes) == 1) {
      series_subset$methane[i] <- ch4_fluxes$flux[1]
      error_base$CH4_value <- TRUE
    } else if (nrow(ch4_fluxes) == 0) {
      series_subset$methane[i] <- NA
      error_base$no_CH4_value <- TRUE
    } else {
      series_subset$methane[i] <- ch4_fluxes$flux[1]  # Take first
      error_base$many_CH4_value <- TRUE
    }
    
    # Add to errors dataframe
    errors_df <- rbind(errors_df, data.frame(error_base, stringsAsFactors = FALSE))
  }
  
  # Reshape data to long format
  series_subset_long <- series_subset %>%
    pivot_longer(
      cols = c(respiration, methane),
      names_to = "gas",
      values_to = "personal_flux"
    ) %>%
    mutate(
      gas = ifelse(gas == "respiration", "co2", "ch4"),
      unit = ifelse(gas == "co2", "ppm", "ppb")
    )
  
  # FIX: Properly handle autotrim mapping after pivot_longer
  # Autotrim only applies to CO2 measurements
  autotrim_flux <- ifelse(series_subset_long$gas == "co2" & series_subset_long$autotrimmed, 
                          series_subset_long$personal_flux, NA)
  personal_trim_flux <- ifelse(series_subset_long$gas == "co2" & !series_subset_long$autotrimmed, 
                               series_subset_long$personal_flux, 
                               ifelse(series_subset_long$gas == "ch4", series_subset_long$personal_flux, NA))
  
  # Create final dataset
  series_subset_modified <- data.frame(
    id = series_subset_long$id,
    meas = series_subset_long$measurements,
    date = series_subset_long$date,
    siteid = series_subset_long$siteid,
    subsiteid = series_subset_long$subsiteid,
    point = series_subset_long$point, 
    gas = series_subset_long$gas,
    start_time = series_subset_long$start_time,
    end_time = series_subset_long$end_time,
    start_temp = series_subset_long$start_temp,
    end_temp = series_subset_long$end_temp,
    unit = series_subset_long$unit,
    wt = NA,
    pointtype = series_subset_long$pointtype,
    soil_temp_5cm = series_subset_long$t05,
    soil_temp_30cm = NA,
    tsmoisture = series_subset_long$tsmoisture,
    volume = series_subset_long$chamber_vol,
    area = series_subset_long$chamber_area,	
    pad_head = NA,
    pad_tail = NA,
    autotrim_flux = autotrim_flux,
    autotrim_resid = NA,
    personal_flux = personal_trim_flux,
    personal_resid = NA,
    trimmer = NA,
    filename = series_subset_long$filename,
    db_origin = series_subset_long$db_origin,
    stringsAsFactors = FALSE
  )
  
  # Write output files
  write.csv(series_subset, file.path(sub_path, paste0(folder, ".csv")), row.names = FALSE)
  write.csv(series_subset_modified, file.path(sub_path, paste0(folder, "db3like.csv")), row.names = FALSE)
}

# Write error log
write.csv(errors_df, "errors_dbmerge.csv", row.names = FALSE)

cat("Processing complete. Check errors_dbmerge.csv for issues.\n")