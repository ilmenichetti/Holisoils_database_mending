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

# Initialize error tracking with consistent data types
errors_list <- list()
error_counter <- 0

# Helper function to add errors consistently
add_error <- function(co2_val = FALSE, no_co2 = FALSE, many_co2 = FALSE,
                      ch4_val = FALSE, no_ch4 = FALSE, many_ch4 = FALSE,
                      n2o_val = FALSE, no_n2o = FALSE, many_n2o = FALSE,
                      error_msg = "", flux_id = NA, meas_set = "", 
                      db_orig = NA, folder_name = "") {
  error_counter <<- error_counter + 1
  errors_list[[error_counter]] <<- data.frame(
    CO2_value = co2_val, no_CO2_value = no_co2, many_CO2_value = many_co2,
    CH4_value = ch4_val, no_CH4_value = no_ch4, many_CH4_value = many_ch4,
    N2O_value = n2o_val, no_N2O_value = no_n2o, many_N2O_value = many_n2o,
    error = error_msg, fluxID = flux_id, meas_set = meas_set,
    dborigin = db_orig, folder = folder_name,
    stringsAsFactors = FALSE
  )
}

# Main processing loop
for (folder in site_ids) {
  cat("Processing site:", folder, "\n")
  
  sub_path <- file.path(main_folder, folder)
  series_subset <- db_series[db_series$siteid == folder, ]
  
  if (nrow(series_subset) == 0) {
    cat("No series found for site:", folder, "\n")
    next
  }
  
  # FIX: Directory structure - align with new API naming convention
  campaigns_df <- series_subset %>%
    distinct(measurements, db_origin) %>%
    mutate(subfolder = paste0("meas_ID", measurements, "_db", db_origin))
  
  for (j in seq_len(nrow(campaigns_df))) {
    subfolder_path <- file.path(sub_path, campaigns_df$subfolder[j])
    if (!dir.exists(subfolder_path)) {
      dir.create(subfolder_path, recursive = TRUE)
    }
  }
  
  # Initialize additional columns including N2O and autotrimmed per gas
  series_subset$file_id <- NA
  series_subset$subfolder <- NA
  series_subset$device <- NA
  series_subset$chamber <- NA
  series_subset$respiration <- NA
  series_subset$methane <- NA
  series_subset$n2o <- NA
  series_subset$sample_name <- series_subset$point
  series_subset$filename <- NA
  # FIX: Autotrimmed per gas instead of overall
  series_subset$co2_autotrimmed <- FALSE
  series_subset$ch4_autotrimmed <- FALSE
  series_subset$n2o_autotrimmed <- FALSE
  
  # Treatment info is contained in subsiteid, point, pointtype - no separate extraction needed
  series_subset$treatment <- NA
  
  # Process each series
  for (i in seq_len(nrow(series_subset))) {
    series_id <- series_subset$id[i]
    # FIX: Directory structure - align with new API
    subfolder <- paste0("meas_ID", series_subset$measurements[i], "_db", series_subset$db_origin[i])
    subfolder_path <- file.path(sub_path, subfolder)
    
    # FIX: Create filename with timing to avoid collisions
    start_time_clean <- gsub(":", "-", series_subset$start_time[i])
    end_time_clean <- gsub(":", "-", series_subset$end_time[i])
    
    filename <- paste(
      series_subset$date[i],
      series_subset$subsiteid[i],
      series_subset$point[i],
      series_subset$pointtype[i],
      start_time_clean,
      end_time_clean,
      sep = "_"
    )
    
    # Write data file if measurements exist
    file_to_write <- db_data[db_data$series == series_id, ]
    
    if (nrow(file_to_write) > 0) {
      # FIX: Feather file structure - standardize column names  
      colnames(file_to_write)[colnames(file_to_write) == "co2_ppm"] <- "co2"
      colnames(file_to_write)[colnames(file_to_write) == "ch4_ppb"] <- "ch4" 
      colnames(file_to_write)[colnames(file_to_write) == "n2o_ppb"] <- "n2o"
      
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
    
    if (nrow(co2_fluxes) == 1) {
      series_subset$respiration[i] <- co2_fluxes$flux[1]
      series_subset$co2_autotrimmed[i] <- co2_fluxes$trimmer[1] == 1
      add_error(co2_val = TRUE, error_msg = paste("CO2 flux found, series", series_id),
                flux_id = series_id, meas_set = subfolder, 
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else if (nrow(co2_fluxes) == 0) {
      series_subset$respiration[i] <- NA
      add_error(no_co2 = TRUE, error_msg = paste("No CO2 flux found for series", series_id),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else {
      # Handle multiple flux values
      series_subset$respiration[i] <- co2_fluxes$flux[1]  # Take first
      series_subset$co2_autotrimmed[i] <- co2_fluxes$trimmer[1] == 1
      add_error(many_co2 = TRUE, error_msg = paste("Multiple CO2 fluxes found for series", series_id, "- using first"),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    }
    
    # Process CH4 flux (gas == 2)
    ch4_fluxes <- db_flux[db_flux$gas == 2 & 
                            db_flux$series == series_id & 
                            db_flux$db_origin == series_subset$db_origin[i], ]
    
    if (nrow(ch4_fluxes) == 1) {
      series_subset$methane[i] <- ch4_fluxes$flux[1]
      series_subset$ch4_autotrimmed[i] <- ch4_fluxes$trimmer[1] == 1
      add_error(ch4_val = TRUE, error_msg = paste("CH4 flux found, series", series_id),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else if (nrow(ch4_fluxes) == 0) {
      series_subset$methane[i] <- NA
      add_error(no_ch4 = TRUE, error_msg = paste("No CH4 flux found for series", series_id),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else {
      series_subset$methane[i] <- ch4_fluxes$flux[1]  # Take first
      series_subset$ch4_autotrimmed[i] <- ch4_fluxes$trimmer[1] == 1
      add_error(many_ch4 = TRUE, error_msg = paste("Multiple CH4 fluxes found for series", series_id, "- using first"),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    }
    
    # Process N2O flux (gas == 3)
    n2o_fluxes <- db_flux[db_flux$gas == 3 & 
                            db_flux$series == series_id & 
                            db_flux$db_origin == series_subset$db_origin[i], ]
    
    if (nrow(n2o_fluxes) == 1) {
      series_subset$n2o[i] <- n2o_fluxes$flux[1]
      series_subset$n2o_autotrimmed[i] <- n2o_fluxes$trimmer[1] == 1
      add_error(n2o_val = TRUE, error_msg = paste("N2O flux found, series", series_id),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else if (nrow(n2o_fluxes) == 0) {
      series_subset$n2o[i] <- NA
      add_error(no_n2o = TRUE, error_msg = paste("No N2O flux found for series", series_id),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    } else {
      series_subset$n2o[i] <- n2o_fluxes$flux[1]  # Take first
      series_subset$n2o_autotrimmed[i] <- n2o_fluxes$trimmer[1] == 1
      add_error(many_n2o = TRUE, error_msg = paste("Multiple N2O fluxes found for series", series_id, "- using first"),
                flux_id = series_id, meas_set = subfolder,
                db_orig = series_subset$db_origin[i], folder_name = folder)
    }
  }
  
  # Reshape data to long format including N2O
  series_subset_long <- series_subset %>%
    pivot_longer(
      cols = c(respiration, methane, n2o),
      names_to = "gas",
      values_to = "personal_flux"
    ) %>%
    mutate(
      gas = case_when(
        gas == "respiration" ~ "co2",
        gas == "methane" ~ "ch4", 
        gas == "n2o" ~ "n2o",
        TRUE ~ gas
      ),
      unit = case_when(
        gas == "co2" ~ "ppm",
        gas == "ch4" ~ "ppb",
        gas == "n2o" ~ "ppb",
        TRUE ~ "unknown"
      ),
      # FIX: Map autotrimmed status per gas
      autotrimmed = case_when(
        gas == "co2" ~ co2_autotrimmed,
        gas == "ch4" ~ ch4_autotrimmed,
        gas == "n2o" ~ n2o_autotrimmed,
        TRUE ~ FALSE
      )
    )
  
  # Create final dataset with proper flux assignments per gas and extracted database values
  final_data_list <- list()
  
  for (i in seq_len(nrow(series_subset_long))) {
    row <- series_subset_long[i, ]
    series_id <- row$id
    gas_type <- row$gas
    db_origin <- row$db_origin
    
    # Get flux data for this specific series and gas
    flux_data <- db_flux[db_flux$series == series_id & 
                           db_flux$gas == case_when(
                             gas_type == "co2" ~ 1,
                             gas_type == "ch4" ~ 2, 
                             gas_type == "n2o" ~ 3,
                             TRUE ~ -1
                           ) & 
                           db_flux$db_origin == db_origin, ]
    
    # Extract real values from db_flux instead of hardcoding NA
    if (nrow(flux_data) > 0) {
      # Use first flux record for this series/gas combination
      flux_row <- flux_data[1, ]
      
      pad_head_val <- flux_row$trim_head
      pad_tail_val <- flux_row$trim_tail
      trimmer_val <- ifelse(flux_row$trimmer == 1, "autotrimmer", "manual")
      
      # FIX: Assign flux and residuals based on trimmer type for ALL gases
      if (flux_row$trimmer == 1) {
        autotrim_flux_val <- flux_row$flux
        autotrim_resid_val <- flux_row$resid
        personal_flux_val <- NA
        personal_resid_val <- NA
      } else {
        autotrim_flux_val <- NA
        autotrim_resid_val <- NA
        personal_flux_val <- flux_row$flux
        personal_resid_val <- flux_row$resid
      }
    } else {
      # No flux data found
      pad_head_val <- NA
      pad_tail_val <- NA  
      trimmer_val <- NA
      autotrim_flux_val <- NA
      autotrim_resid_val <- NA
      personal_flux_val <- NA
      personal_resid_val <- NA
    }
    
    # Get environmental data from series data
    series_row <- series_subset[series_subset$id == series_id, ]
    wt_val <- if (nrow(series_row) > 0) series_row$wt[1] else NA
    treatment_val <- if (nrow(series_row) > 0) series_row$treatment[1] else NA
    
    # Create final row
    final_data_list[[i]] <- data.frame(
      id = row$id,
      meas = row$measurements,
      date = row$date,
      siteid = row$siteid,
      subsiteid = row$subsiteid,
      point = row$point,
      treatment = treatment_val,  # FIX: Extract treatment if available
      gas = row$gas,
      start_time = row$start_time,
      end_time = row$end_time,
      start_temp = row$start_temp,
      end_temp = row$end_temp,
      unit = row$unit,
      wt = wt_val,
      pointtype = row$pointtype,
      soil_temp_5cm = row$t05,
      soil_temp_30cm = NA,  # Still NA as t30 was dropped
      tsmoisture = row$tsmoisture,
      volume = row$chamber_vol,
      area = row$chamber_area,	
      pad_head = pad_head_val,
      pad_tail = pad_tail_val,
      autotrim_flux = autotrim_flux_val,
      autotrim_resid = autotrim_resid_val,
      personal_flux = personal_flux_val,
      personal_resid = personal_resid_val,
      trimmer = trimmer_val,
      filename = row$filename,
      db_origin = row$db_origin,
      stringsAsFactors = FALSE
    )
  }
  
  # Combine all rows
  series_subset_modified <- do.call(rbind, final_data_list)
  
  # Write output files
  write.csv(series_subset, file.path(sub_path, paste0(folder, ".csv")), row.names = FALSE)
  write.csv(series_subset_modified, file.path(sub_path, paste0(folder, "db3like.csv")), row.names = FALSE)
}

# Combine errors consistently
if (length(errors_list) > 0) {
  errors_df <- do.call(rbind, errors_list)
} else {
  errors_df <- data.frame(
    CO2_value = logical(0), no_CO2_value = logical(0), many_CO2_value = logical(0),
    CH4_value = logical(0), no_CH4_value = logical(0), many_CH4_value = logical(0),
    N2O_value = logical(0), no_N2O_value = logical(0), many_N2O_value = logical(0),
    error = character(0), fluxID = numeric(0), meas_set = character(0),
    dborigin = numeric(0), folder = character(0),
    stringsAsFactors = FALSE
  )
}

# Write error log
write.csv(errors_df, "errors_dbmerge.csv", row.names = FALSE)

cat("Processing complete. Check errors_dbmerge.csv for issues.\n")