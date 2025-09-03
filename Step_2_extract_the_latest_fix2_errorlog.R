writefiles=T
main_folder = "./Holisoils_GHG_data"

library(tidyverse)
library(httr2)
library(arrow)
library(lubridate)
library(tictoc)
library(progress)

#set the working dir where this script is located (just in case)
library(here)
setwd(here::here())

# ----------------------------------------------------------------------------------------
# Initialize error logging
# ----------------------------------------------------------------------------------------

# Initialize the log file (clear previous contents)
log_file <- "errors_dbremote.txt"
if(file.exists(log_file)) {
  file.remove(log_file)
}

# Create header for log file
cat("=== GHG Data Processing Error Log ===", "\n", file = log_file, append = FALSE)
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n", file = log_file, append = TRUE)

# Function to log errors/warnings with identifiers
log_error <- function(message, measurement_id = NA, series_id = NA, site_id = NA, subsiteid = NA, point = NA, gas = NA, additional_info = "") {
  # Create identifier string
  identifier_parts <- c()
  if(!is.na(measurement_id)) identifier_parts <- c(identifier_parts, paste("MeasID:", measurement_id))
  if(!is.na(series_id)) identifier_parts <- c(identifier_parts, paste("SeriesID:", series_id))
  if(!is.na(site_id)) identifier_parts <- c(identifier_parts, paste("Site:", site_id))
  if(!is.na(subsiteid)) identifier_parts <- c(identifier_parts, paste("SubSite:", subsiteid))
  if(!is.na(point)) identifier_parts <- c(identifier_parts, paste("Point:", point))
  if(!is.na(gas)) identifier_parts <- c(identifier_parts, paste("Gas:", gas))
  if(additional_info != "") identifier_parts <- c(identifier_parts, additional_info)
  
  identifier_string <- if(length(identifier_parts) > 0) paste("[", paste(identifier_parts, collapse = ", "), "]") else "[General]"
  
  # Create full message
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_message <- paste(timestamp, identifier_string, message)
  
  # Print to console
  cat(full_message, "\n")
  
  # Write to log file
  cat(full_message, "\n", file = log_file, append = TRUE)
}

# ----------------------------------------------------------------------------------------
# helper functions
# ----------------------------------------------------------------------------------------

# Helper function to convert mixed data to numeric with proper NAs
to_numeric_with_na <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_real_)
  }
  
  # If already numeric, handle NaN/Inf
  if (is.numeric(x)) {
    return(ifelse(is.finite(x), x, NA_real_))
  }
  
  # Convert to character first to handle factors
  x <- as.character(x)
  
  # Define missing value patterns
  missing_patterns <- c("nan", "no data", "", "na", "null", "missing", "n/a")
  
  # Check if it's a missing value pattern (case insensitive)
  if (tolower(trimws(x)) %in% missing_patterns) {
    return(NA_real_)
  }
  
  # Try to convert to numeric
  result <- suppressWarnings(as.numeric(x))
  
  # Return NA if conversion failed
  return(ifelse(is.na(result), NA_real_, result))
}


# ----------------------------------------------------------------------------------------
# get the API credentials (locally stored, this is to send the code without security leaks)
# ----------------------------------------------------------------------------------------
api_username <- Sys.getenv("ghgportal_API_USERNAME")
api_password <- Sys.getenv("ghgportal_API_PASSWORD")

# ----------------------------------------------------------------------------------------
# define the API settings and functions
# ----------------------------------------------------------------------------------------
access <- list(
  protocol = "https",
  ip = "chambers.ghgportal.luke.fi",
  ping_addr = "/api/ping/",
  auth_addr = "/api/auth/login/",
  wa_addr = "/api/workerassignments/",
  meas_addr = "/api/measurements/",
  series_addr = "/api/series/",
  flux_addr = "/api/flux/"
)

req <- request(paste(access$protocol,"://",access$ip,access$ping_addr,sep=""))
resp <- req_perform(req)

get_token <- function(){
  # uname <- readline("enter your username: ")
  # upass <- readline("enter password: ")
  uname <- api_username
  upass <- api_password
  token_url <- paste(access$protocol,"://",access$ip,access$auth_addr,sep="")
  token_req <- request(token_url) |> req_method("POST") |> req_auth_basic(uname,upass)
  resp <- req_perform(token_req)
  resp_json <- resp |> resp_body_json()
  print(resp_json)
  return(resp_json)
}

get_projects <- function(token){
  req <- request(paste(access$protocol,"://",access$ip,access$wa_addr,sep=""))
  req <- req |> req_headers(Authorization = paste("Token",token))
  resp <- req_perform(req)
  resp_json <- resp |> resp_body_json()
  wa <- do.call(rbind.data.frame, resp_json$workerassignments)
  return(wa)
}

get_meas <- function(token,project_id){
  req_url <- paste(access$protocol,"://",access$ip,access$meas_addr,project_id,sep="")
  req <- request(req_url) |> req_headers(Authorization = paste("Token",token))
  resp <- req_perform(req)
  resp_json <- resp |> resp_body_json()
  meas <- do.call(rbind.data.frame, resp_json$measurements)
  return(meas)
}

get_series <- function(token,meas_id){
  req_url <- paste(access$protocol,"://",access$ip,access$series_addr,meas_id,sep="")
  req <- request(req_url) |> req_headers(Authorization = paste("Token",token))
  resp <- req_perform(req)
  resp_json <- resp |> resp_body_json()
  return(resp_json$series)
}

get_flux <- function(token,series_id){
  req_url <- paste(access$protocol,"://",access$ip,access$flux_addr,series_id,sep="")
  req <- request(req_url) |> req_headers(Authorization = paste("Token",token))
  resp <- req_perform(req)
  resp_json <- resp |> resp_body_json()
  return(resp_json$flux)
}

process_single_series <- function(series_list,index){
  part <- series_list[[index]]
  values <- unlist(part$values)
  part$values <- values
  return(part)
}

get_token()

tok <- "ce906a90cab4b432ea3f4c07d393199c"
tok <- get_token()$token

wa <- get_projects(tok)



# ----------------------------------------------------------------------------------------
# extract the summary tables and check a bit
# ----------------------------------------------------------------------------------------

meas <- as_tibble(get_meas(tok,1))


##meas %>% filter(status != "accepted") %>% print(n=Inf)
meas_ids <- meas %>% filter(status == "accepted") %>% pull(id)
meas_sites <- meas %>% filter(status == "accepted") %>% pull(siteids)
accepted_ids <- meas %>% filter(status == "accepted") %>% pull(siteids)
rejected_ids <- meas %>% filter(status != "accepted") %>% pull(siteids)
rejected_uploaders <- meas %>% filter(status != "accepted") %>% pull(measurer_name)


unique(meas$siteids)
colnames(meas)


#### checking a bit what is inside
png("check_sites_measurements.png", width = 2800, height = 2600, res=250)
site_ids = unique(meas_sites)
par(mfrow = rep(round(sqrt(length(site_ids)))+1, 2))

# Define colors for the categories
colors = c("accepted" = "lightblue", "rejected" = "lightcoral")

plotting_table = list()
for(i in 1:length(site_ids)){
  site_now = site_ids[i]
  
  # Filter data for current site
  site_data = meas[meas$siteids == site_now, ]
  
  # Create status categories (accepted vs rejected)
  site_data$status_clean = ifelse(site_data$status == "accepted", "accepted", "rejected")
  
  # Create year-month column
  site_data$year_month = format(as.Date(site_data$date), "%Y-%m")
  
  # Create contingency table: rows = year_month, columns = status
  plotting_table[[i]] = table(site_data$year_month, site_data$status_clean)
  
  # Ensure both categories are present (even if one has 0 counts)
  if(!"accepted" %in% colnames(plotting_table[[i]])) {
    plotting_table[[i]] = cbind(plotting_table[[i]], accepted = 0)
  }
  if(!"rejected" %in% colnames(plotting_table[[i]])) {
    plotting_table[[i]] = cbind(plotting_table[[i]], rejected = 0)
  }
  
  # Reorder columns to ensure consistent order
  plotting_table[[i]] = plotting_table[[i]][, c("accepted", "rejected")]
  
  # Create grouped barplot
  barplot(t(plotting_table[[i]]), 
          main = site_now,
          beside = TRUE,
          col = colors,
          legend = if(i == 1) c("Accepted", "Rejected") else FALSE,
          args.legend = list(x = "topright", cex = 0.8),
          las = 2,  # Rotate x-axis labels
          cex.names = 0.7)  # Smaller font for month labels
}
dev.off()


#check the rejected uploads
write.csv(meas[meas$status != "accepted",], "rejected_uploads.csv")

#this in order to get the mechanism for writing the files to work
meas_sites[order(meas_sites)]
meas_ids <- meas_ids[order(meas_sites)]



# ----------------------------------------------------------------------------------------
# process all series
# ----------------------------------------------------------------------------------------


## this collects the individual measured series and their calculated flux values

#loop running for each measurement id
# Initialize diff_vecs before the main loop
diff_vecs <- numeric(length(meas_ids))

# Main loop running for each measurement id
# MEASUREMENT ID LEVEL: Each measurement ID represents a data collection session
for(i in 1:length(meas_ids)){
  
  cat("Processing measurement ID", i, "of", length(meas_ids), "- ID:", meas_ids[i], "\n")
  cat("Downloading...", "\n")
  
  tic() # bit of benchmarking...
  
  # Error handling for series download
  tryCatch({
    s0 <- get_series(tok, meas_ids[i])
  }, error = function(e) {
    log_error(paste("Failed to download series data:", e$message), 
              measurement_id = meas_ids[i])
    next
  })
  
  toc()
  
  # Check if series data is valid
  if(length(s0) == 0) {
    log_error("Empty series data received from API", 
              measurement_id = meas_ids[i])
    next
  }
  
  # series summary in tabular form, loop should run for each row
  tryCatch({
    series_table <- data.frame(
      id = sapply(s0, `[[`, "id"),
      measurements = sapply(s0, `[[`, "measurements"),
      date = sapply(s0, `[[`, "date"),
      siteid = sapply(s0, `[[`, "siteid"),
      subsiteid = sapply(s0, `[[`, "subsiteid"),
      point = sapply(s0, `[[`, "point"),
      gas = sapply(s0, `[[`, "gas"),
      start_time = sapply(s0, `[[`, "start_time"),
      end_time = sapply(s0, `[[`, "end_time"),
      start_temp = sapply(s0, `[[`, "start_temp"),
      end_temp = sapply(s0, `[[`, "end_temp"),
      unit = sapply(s0, `[[`, "unit"),
      volume = sapply(s0, `[[`, "volume"),
      area = sapply(s0, `[[`, "area"),
      pad_head = sapply(s0, `[[`, "pad_head"),
      pad_tail = sapply(s0, `[[`, "pad_tail"),
      env_wt = sapply(s0, function(x) x$env$wt),
      env_point_type = sapply(s0, function(x) x$env$`point type`),
      env_soil_temp_5cm = sapply(s0, function(x) x$env$soil_temp_5cm),
      env_soil_temp_30cm = sapply(s0, function(x) x$env$soil_temp_30cm),
      n_values = sapply(s0, function(x) length(x$values)),
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    log_error(paste("Failed to create series table:", e$message), 
              measurement_id = meas_ids[i])
    next
  })
  
  # Which sites were measured in the current series?
  sites_in_series <- unique(series_table$siteid)
  
  # STORAGE FOR MULTI-GAS PROCESSING:
  # subseries_data: List storing all subseries info for later grouping by instrument session
  # site_datafiles: List storing site-level dataframes for CSV output
  subseries_data <- list()
  site_datafiles <- list()
  
  # Pre-initialize all site datafiles for this measurement
  for(site_name in sites_in_series) {
    # Define paths immediately after site_name
    datafile_path <- file.path(main_folder, site_name)
    datafile_path_file <- file.path(main_folder, site_name, site_name)
    
    # Read if the datafile already exists
    tryCatch({
      if(file.exists(file.path(datafile_path, paste0(site_name, "whole.csv")))) {
        site_datafile <- read.csv(file.path(datafile_path, paste0(site_name, "whole.csv")))
        
        # Add missing treatment column if it doesn't exist
        if(!"treatment" %in% names(site_datafile)) {
          site_datafile$treatment <- NA_character_
          log_error("Added missing 'treatment' column",
                    measurement_id = meas_ids[i], site_id = site_name)
        }
        
        # Add missing filename column if it doesn't exist
        if(!"filename" %in% names(site_datafile)) {
          site_datafile$filename <- NA_character_
          log_error("Added missing 'filename' column",
                    measurement_id = meas_ids[i], site_id = site_name)
        }
        
        # Convert to tibble
        site_datafile <- as_tibble(site_datafile)
        
      } else {
        log_error("Initialised file not found, initialising data structure",
                  measurement_id = meas_ids[i], site_id = site_name)
        
        # Initialize the data structure for the current site
        site_datafile <- tibble(
          id=integer(), 
          meas=integer(), 
          date=character(),
          siteid=character(), 
          subsiteid=character(), 
          point=character(),
          treatment=character(),
          gas=character(), 
          start_time=character(), 
          end_time=character(),
          start_temp=double(),
          end_temp=double(),
          unit=character(),
          wt=double(),
          pointtype=character(),
          soil_temp_5cm=double(),
          soil_temp_30cm=double(),
          tsmoisture=double(),
          volume=double(), 
          area=double(), 
          pad_head=integer(), 
          pad_tail=integer(),
          autotrim_flux=double(), 
          autotrim_resid=double(),
          personal_flux=double(), 
          personal_resid=double(), 
          trimmer=character(), 
          filename=character()
        )
        
        # Add the db_origin column to the empty structure
        log_error("Combining the initialised data structure with older sources",
                  measurement_id = meas_ids[i], site_id = site_name)
        
        # First step: add the old data in the data structure for current site
        if(file.exists(paste(datafile_path_file, "db3like.csv", sep=""))) {
          old_datafile <- read.csv(paste(datafile_path_file, "db3like.csv", sep=""))[,-1]
          site_datafile$db_origin <- 3
          combined_datafile <- rbind(old_datafile, site_datafile)
        } else {
          combined_datafile <- site_datafile
        }
      } # end of the IF loop for combining the data files
    }, error = function(e) {
      log_error(paste("Failed to initialize site datafile:", e$message),
                measurement_id = meas_ids[i], site_id = site_name)
      # Create minimal structure as fallback
      site_datafile <- tibble(
        id=integer(), meas=integer(), date=character(), siteid=character(), 
        subsiteid=character(), point=character(), treatment=character(),
        gas=character(), start_time=character(), end_time=character(),
        start_temp=double(), end_temp=double(), unit=character(), wt=double(),
        pointtype=character(), soil_temp_5cm=double(), soil_temp_30cm=double(),
        tsmoisture=double(), volume=double(), area=double(), pad_head=integer(), 
        pad_tail=integer(), autotrim_flux=double(), autotrim_resid=double(),
        personal_flux=double(), personal_resid=double(), trimmer=character(), 
        filename=character()
      )
    })
    
    # This is to be sure that all points are stored as character
    site_datafile$point <- as.character(site_datafile$point) 
    site_datafile$pointtype <- as.character(site_datafile$pointtype) 
    site_datafile$wt <- as.numeric(site_datafile$wt) 
    
    # Store the initialized datafile
    site_datafiles[[site_name]] <- site_datafile
  }
  
  # Get total number of subseries across all sites
  total_subseries <- length(s0)
  
  cat("Processing", total_subseries, "total subseries across", length(sites_in_series), "sites\n")
  
  # Set the progress bar for all subseries
  pb <- progress_bar$new(
    format = "  Processing all subseries [:bar] :percent (:current/:total)",
    total = total_subseries, 
    clear = FALSE, 
    width = 60
  )
  
  # SUBSERIES LEVEL: Each subseries represents one gas measurement from one instrument at one point/time
  # Middle loop - running for each subseries across all sites  
  for(j in 1:total_subseries){
    
    # Process the current subseries with error handling
    tryCatch({
      s_obj <- process_single_series(s0, j)
      current_site <- s_obj$siteid
      
      cat(paste("    Processing subseries", j, "for site", current_site, "\n"))
      
      s_id <- s_obj$id
    }, error = function(e) {
      log_error(paste("Failed to process subseries:", e$message),
                measurement_id = meas_ids[i], series_id = if(exists("s_id")) s_id else NA, 
                site_id = if(exists("current_site")) current_site else NA)
      next
    })
    
    # Get flux data with error handling
    tryCatch({
      f_obj <- get_flux(tok, s_id)
    }, error = function(e) {
      log_error(paste("Failed to get flux data:", e$message),
                measurement_id = meas_ids[i], series_id = s_id, 
                site_id = current_site, subsiteid = s_obj$subsiteid, 
                point = s_obj$point, gas = s_obj$gas)
      f_obj <- list() # Set empty flux object to continue processing
    })
    
    # Process flux calculations with error handling
    tryCatch({
      if(length(f_obj) == 1){
        a_flux <- f_obj[[1]]$flux
        a_resid <- f_obj[[1]]$resid
        p_flux <- NA
        p_resid <- NA
        trimmer <- NA
      } else if(length(f_obj) > 1){
        trimmers <- character(length(f_obj))
        for(m in 1:length(f_obj)){
          trimmers[m] <- f_obj[[m]]$trimmer_name
        }
        auto_id <- which(trimmers == "autotrimmer")
        other_id <- which(trimmers != "autotrimmer")
        a_flux <- f_obj[[auto_id[1]]]$flux
        a_resid <- f_obj[[auto_id[1]]]$resid
        if(length(other_id) > 0){
          flux_values <- numeric(length(other_id))
          for(m in 1:length(other_id)){
            flux_values[m] <- f_obj[[other_id[m]]]$flux  # Fixed indexing
          }
          p_flux <- mean(flux_values)
          if(length(other_id) == 1){
            trimmer <- trimmers[other_id[1]]
            p_resid <- f_obj[[other_id[1]]]$resid
          } else {
            trimmer <- "multiple"
            p_resid <- NA
          }
        } else {
          p_flux <- NA
          p_resid <- NA
          trimmer <- NA
        }
      } else {
        a_flux <- NA
        a_resid <- NA
        p_flux <- NA
        p_resid <- NA
        trimmer <- NA
        log_error("Empty flux object received",
                  measurement_id = meas_ids[i], series_id = s_id,
                  site_id = current_site, subsiteid = s_obj$subsiteid,
                  point = s_obj$point, gas = s_obj$gas)
      }
    }, error = function(e) {
      log_error(paste("Failed to process flux calculations:", e$message),
                measurement_id = meas_ids[i], series_id = s_id,
                site_id = current_site, subsiteid = s_obj$subsiteid,
                point = s_obj$point, gas = s_obj$gas)
      # Set default values
      a_flux <- NA; a_resid <- NA; p_flux <- NA; p_resid <- NA; trimmer <- NA
    })
    
    # Generate sequence of 1-second intervals with error handling
    tryCatch({
      start_time <- as.POSIXct(paste(s_obj$date, s_obj$start_time), format = "%Y-%m-%d %H:%M:%S")
      end_time <- as.POSIXct(paste(s_obj$date, s_obj$end_time), format = "%Y-%m-%d %H:%M:%S")
      time_sequence <- seq(from = start_time, to = end_time, by = "1 sec")
      time_sequence_formatted <- format(time_sequence, "%H:%M:%S")
      trimmed_values <- s_obj$values[(s_obj$pad_head + 1):(length(s_obj$values) - s_obj$pad_tail)]
    }, error = function(e) {
      log_error(paste("Failed to generate time sequence:", e$message),
                measurement_id = meas_ids[i], series_id = s_id,
                site_id = current_site, subsiteid = s_obj$subsiteid,
                point = s_obj$point, gas = s_obj$gas)
      # Set minimal fallback values
      time_sequence_formatted <- character(0)
      trimmed_values <- numeric(0)
    })
    
    # Criterium is 5 steps now
    length_diff = length(trimmed_values) - length(time_sequence_formatted)
    
    if (length_diff > 0){
      if (length_diff > 5) {
        log_error(paste("Measurements", length_diff, "shorter than time vector"),
                  measurement_id = meas_ids[i], series_id = s_id,
                  site_id = current_site, subsiteid = s_obj$subsiteid,
                  point = s_obj$point, gas = s_obj$gas)
      }
      diff_vecs[i] = length_diff
      trimmed_values = trimmed_values[(1 + diff_vecs[i]):length(trimmed_values)]
    } else if (length_diff < 0) {
      if (abs(length_diff) > 5) {
        log_error(paste("Measurements", abs(length_diff), "longer than time vector"),
                  measurement_id = meas_ids[i], series_id = s_id,
                  site_id = current_site, subsiteid = s_obj$subsiteid,
                  point = s_obj$point, gas = s_obj$gas)
      }
      diff_vecs[i] = abs(length_diff)
      time_sequence_formatted = time_sequence_formatted[(1 + diff_vecs[i]):length(time_sequence_formatted)]
    }
    
    # Create the name of the file to write - include timing to separate measurement sessions
    tryCatch({
      filename_featherfile = paste(s_obj$date,
                                   s_obj$subsiteid,
                                   s_obj$point,
                                   s_obj$env$`point type`,
                                   s_obj$start_time,
                                   s_obj$end_time, sep="_")
      
      # Clean up filename if it contains problematic characters
      # suppressing error message because it is too verbose
      # if (grepl("/", filename_featherfile)) {
      #   filename_featherfile <- gsub("/", "-", filename_featherfile)
      #   log_error("Cleaned up filename containing forward slashes",
      #             measurement_id = meas_ids[i], series_id = s_id,
      #             site_id = current_site, subsiteid = s_obj$subsiteid,
      #             point = s_obj$point, gas = s_obj$gas,
      #             additional_info = paste("New filename:", filename_featherfile))
      # }
      if (grepl("/", filename_featherfile)) {
        filename_featherfile <- gsub("/", "-", filename_featherfile)
      }
      # Clean up colons in time format for filename compatibility
      filename_featherfile <- gsub(":", "-", filename_featherfile)
    }, error = function(e) {
      log_error(paste("Failed to create filename:", e$message),
                measurement_id = meas_ids[i], series_id = s_id,
                site_id = current_site, subsiteid = s_obj$subsiteid,
                point = s_obj$point, gas = s_obj$gas)
      filename_featherfile <- paste("error_file", s_id, sep="_")
    })
    
    # SITE LEVEL: Inner loop - processing sites for current subseries
    # (Usually just one site per subseries, but keeping loop structure as requested)
    for(k in which(sites_in_series == current_site)){
      
      site_name <- sites_in_series[k]
      
      cat(paste("      Processing for site", site_name, "(site", k, "of", length(sites_in_series), ")\n"))
      
      # Define paths
      datafile_path <- file.path(main_folder, site_name)
      
      # Add row to the appropriate site datafile with error handling
      tryCatch({
        site_datafiles[[site_name]] <- site_datafiles[[site_name]] %>% add_row(
          id = s_obj$id,
          meas = s_obj$measurements,
          date = s_obj$date,
          siteid = s_obj$siteid,
          subsiteid = s_obj$subsiteid,
          point = as.character(s_obj$point),
          treatment = as.character(ifelse(is.null(s_obj$env$treatment), NA, s_obj$env$treatment)),
          gas = s_obj$gas,
          start_time = s_obj$start_time,
          end_time = s_obj$end_time,
          start_temp = to_numeric_with_na(s_obj$start_temp),
          end_temp = to_numeric_with_na(s_obj$end_temp),
          unit = s_obj$unit,
          wt = to_numeric_with_na(s_obj$env$wt),
          pointtype = as.character(s_obj$env$`point type`),
          soil_temp_5cm = to_numeric_with_na(s_obj$env$soil_temp_5cm),
          soil_temp_30cm = to_numeric_with_na(s_obj$env$soil_temp_30cm),
          tsmoisture = to_numeric_with_na(s_obj$env$tsmoisture),
          volume = to_numeric_with_na(s_obj$volume),
          area = to_numeric_with_na(s_obj$area),
          pad_head = as.integer(to_numeric_with_na(s_obj$pad_head)),
          pad_tail = as.integer(to_numeric_with_na(s_obj$pad_tail)),
          autotrim_flux = a_flux,
          autotrim_resid = a_resid,
          personal_flux = p_flux,
          personal_resid = p_resid,
          trimmer = as.character(ifelse(is.null(trimmer) || is.na(trimmer), NA, trimmer)),
          filename = filename_featherfile
        )
      }, error = function(e) {
        log_error(paste("Failed to add row to site datafile:", e$message),
                  measurement_id = meas_ids[i], series_id = s_id,
                  site_id = current_site, subsiteid = s_obj$subsiteid,
                  point = s_obj$point, gas = s_obj$gas)
      })
      
      # Store subseries data for later grouping and feather file creation
      subseries_data[[length(subseries_data) + 1]] <- list(
        s_obj = s_obj,
        site_name = site_name,
        datafile_path = datafile_path,
        filename_featherfile = filename_featherfile,
        trimmed_values = trimmed_values,
        time_sequence_formatted = time_sequence_formatted
      )
      
    } # closing k loop (now innermost - processing sites for current subseries)
    
    pb$tick()
    
  } # closing j loop (now middle - running for each subseries across all sites)
  
  # FEATHER FILE GROUPING LEVEL: Process multi-gas measurements
  # A "GROUP" represents all gas measurements from the same instrument session
  # - Same measurement location (date + subsiteid + point + point_type + start_time + end_time)  
  # - Same timing (simultaneous measurements of different gases)
  # - Results in ONE combined feather file with multiple gas columns
  cat("Processing feather files with multi-gas support...\n")
  
  # Group subseries data by filename (now includes timing to separate measurement sessions)
  # Each group = one instrument session potentially measuring multiple gases (co2, ch4, n2o)
  grouped_data <- split(subseries_data, sapply(subseries_data, function(x) x$filename_featherfile))
  
  for(group_name in names(grouped_data)) {
    group <- grouped_data[[group_name]]
    # GROUP CONTENTS: All gas measurements from one instrument session
    # - Should have identical timing (same start/end time, padding, time sequence)
    # - Should have different gas types (co2, ch4, n2o) 
    # - Will be combined into one feather file with gas-specific columns
    
    cat(paste("  Processing group:", group_name, "with", length(group), "gas measurements\n"))
    
    # Extract gas types and check for consistency with error handling
    tryCatch({
      gas_types <- sapply(group, function(x) x$s_obj$gas)
      unique_gases <- unique(gas_types)
      
      # Check for duplicate gas types and handle them
      if(length(gas_types) != length(unique_gases)) {
        log_error("Duplicate gas types detected - will average duplicate measurements",
                  measurement_id = meas_ids[i],
                  additional_info = paste("Group:", group_name))
        for(gas in unique_gases) {
          duplicates <- which(gas_types == gas)
          if(length(duplicates) > 1) {
            log_error(paste("Gas", gas, "has", length(duplicates), "measurements - will average them"),
                      measurement_id = meas_ids[i], gas = gas,
                      additional_info = paste("Group:", group_name))
            for(dup_i in duplicates) {
              s_obj <- group[[dup_i]]$s_obj
              log_error(paste("Duplicate series", s_obj$id, ": measurement_id =", s_obj$measurements),
                        measurement_id = meas_ids[i], series_id = s_obj$id,
                        site_id = s_obj$siteid, subsiteid = s_obj$subsiteid,
                        point = s_obj$point, gas = gas)
            }
          }
        }
      }
      
      # Check for unexpected gas types and maximum gas limit
      expected_gases <- c("co2", "ch4", "n2o")
      unexpected <- setdiff(unique_gases, expected_gases)
      if(length(unexpected) > 0) {
        log_error(paste("Unexpected gas types found:", paste(unexpected, collapse=", ")),
                  measurement_id = meas_ids[i],
                  additional_info = paste("Group:", group_name))
      }
      
      # Check maximum gas limit (should be max 3 gases per measurement session)
      if(length(unique_gases) > 3) {
        log_error(paste("Too many gas types in group - maximum 3 expected, found:", length(unique_gases)),
                  measurement_id = meas_ids[i],
                  additional_info = paste("Group:", group_name))
      }
      
      # Check gas name consistency within each gas type
      for(gas in unique_gases) {
        gas_instances <- gas_types[gas_types == gas]
        if(length(unique(gas_instances)) > 1) {
          log_error(paste("Inconsistent gas naming for", gas, ":", paste(unique(gas_instances), collapse=", ")),
                    measurement_id = meas_ids[i], gas = gas,
                    additional_info = paste("Group:", group_name))
        }
      }
    }, error = function(e) {
      log_error(paste("Failed to process gas type consistency:", e$message),
                measurement_id = meas_ids[i],
                additional_info = paste("Group:", group_name))
      next
    })
    
    # Verify timing consistency within group - should be identical since we group by timing
    if(length(group) > 1) {
      tryCatch({
        reference_subseries <- group[[1]]$s_obj
        reference_time_seq <- group[[1]]$time_sequence_formatted
        
        for(j_inner in 2:length(group)) {
          curr <- group[[j_inner]]$s_obj
          curr_time_seq <- group[[j_inner]]$time_sequence_formatted
          
          # With corrected grouping, timing should be identical by design
          if(curr$date != reference_subseries$date || 
             curr$start_time != reference_subseries$start_time || 
             curr$end_time != reference_subseries$end_time ||
             curr$pad_head != reference_subseries$pad_head ||
             curr$pad_tail != reference_subseries$pad_tail) {
            log_error("Timing mismatch in group - grouping logic error",
                      measurement_id = meas_ids[i], series_id = curr$id,
                      site_id = curr$siteid, subsiteid = curr$subsiteid,
                      point = curr$point, gas = curr$gas,
                      additional_info = paste("Group:", group_name))
          }
          
          # Verify time sequence consistency
          if(length(curr_time_seq) != length(reference_time_seq)) {
            log_error("Time sequence length mismatch in group",
                      measurement_id = meas_ids[i], series_id = curr$id,
                      site_id = curr$siteid, subsiteid = curr$subsiteid,
                      point = curr$point, gas = curr$gas,
                      additional_info = paste("Group:", group_name, "- Expected:", length(reference_time_seq), "Got:", length(curr_time_seq)))
          }
          
          if(!identical(curr_time_seq, reference_time_seq)) {
            log_error("Time sequence values differ in group",
                      measurement_id = meas_ids[i], series_id = curr$id,
                      site_id = curr$siteid, subsiteid = curr$subsiteid,
                      point = curr$point, gas = curr$gas,
                      additional_info = paste("Group:", group_name))
          }
        }
      }, error = function(e) {
        log_error(paste("Failed to verify timing consistency:", e$message),
                  measurement_id = meas_ids[i],
                  additional_info = paste("Group:", group_name))
      })
    }
    
    # Create combined feather file structure with error handling
    tryCatch({
      time_seq <- group[[1]]$time_sequence_formatted
      n_measurements <- length(time_seq)
      
      # Initialize with NA values
      feather_data <- data.frame(
        time = time_seq,
        co2 = rep(NA_real_, n_measurements),
        ch4 = rep(NA_real_, n_measurements),
        n2o = rep(NA_real_, n_measurements),
        series = rep(group[[1]]$s_obj$id, n_measurements),
        db_origin = rep(3, n_measurements),
        stringsAsFactors = FALSE
      )
      
      # Fill in the actual gas measurements, handling duplicates by averaging. 
      # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! There are apparently duplicated CO2 fluxes in Llobera
      # The case was measurement_id 360, series 16906 & 17017 (both CO2 at 09:30:53-09:32:22)
      # measurement_id groups entire daily campaigns, not individual sessions
      # Solution: Average duplicate measurements instead of throwing error 
      for(gas in unique_gases) {
        tryCatch({
          gas_indices <- which(gas_types == gas)
          
          if(length(gas_indices) == 1) {
            # Single measurement for this gas
            values <- group[[gas_indices[1]]]$trimmed_values
            if(length(values) == 0) {
              log_error(paste("Empty measurement series for", gas),
                        measurement_id = meas_ids[i],
                        series_id = group[[gas_indices[1]]]$s_obj$id,
                        site_id = group[[gas_indices[1]]]$s_obj$siteid,
                        subsiteid = group[[gas_indices[1]]]$s_obj$subsiteid,
                        point = group[[gas_indices[1]]]$s_obj$point,
                        gas = gas,
                        additional_info = paste("Group:", group_name))
              # Keep as NA
            } else {
              feather_data[[gas]] <- values
            }
          } else {
            # Multiple measurements for this gas - average them
            log_error(paste("Averaging", length(gas_indices), "measurements for", gas),
                      measurement_id = meas_ids[i], gas = gas,
                      additional_info = paste("Group:", group_name))
            
            # Collect all measurements for this gas
            all_measurements <- list()
            all_valid <- TRUE
            
            for(idx in gas_indices) {
              values <- group[[idx]]$trimmed_values
              if(length(values) == 0) {
                log_error(paste("Empty measurement series in", gas, "duplicate", idx),
                          measurement_id = meas_ids[i],
                          series_id = group[[idx]]$s_obj$id,
                          site_id = group[[idx]]$s_obj$siteid,
                          subsiteid = group[[idx]]$s_obj$subsiteid,
                          point = group[[idx]]$s_obj$point,
                          gas = gas,
                          additional_info = paste("Group:", group_name))
                all_valid <- FALSE
                break
              }
              all_measurements[[length(all_measurements) + 1]] <- values
            }
            
            if(all_valid && length(all_measurements) > 0) {
              # Check that all measurements have the same length
              lengths <- sapply(all_measurements, length)
              if(length(unique(lengths)) == 1) {
                # Average across measurements
                averaged_values <- Reduce(`+`, all_measurements) / length(all_measurements)
                feather_data[[gas]] <- averaged_values
                cat(paste("    Successfully averaged", length(gas_indices), "measurements for", gas, "\n"))
              } else {
                log_error(paste("Cannot average", gas, "measurements - different lengths:", paste(lengths, collapse=", ")),
                          measurement_id = meas_ids[i], gas = gas,
                          additional_info = paste("Group:", group_name))
                # Use the first measurement as fallback
                feather_data[[gas]] <- all_measurements[[1]]
              }
            }
          }
        }, error = function(e) {
          log_error(paste("Failed to process gas measurements for", gas, ":", e$message),
                    measurement_id = meas_ids[i], gas = gas,
                    additional_info = paste("Group:", group_name))
        })
      }
      
      # Write warning for missing gas combinations if this site has multiple gases elsewhere
      all_gases_this_site <- unique(unlist(lapply(subseries_data, function(x) {
        if(x$site_name == group[[1]]$site_name) x$s_obj$gas else NULL
      })))
      
      if(length(all_gases_this_site) > length(unique_gases)) {
        missing_gases <- setdiff(all_gases_this_site, unique_gases)
        log_error(paste("Missing gases found elsewhere:", paste(missing_gases, collapse=", ")),
                  measurement_id = meas_ids[i],
                  site_id = group[[1]]$site_name,
                  additional_info = paste("Group:", group_name))
      }
      
      # Create feather file path and write
      feather_folder_path <- file.path(group[[1]]$datafile_path, paste("meas_ID", meas_ids[i], "db3", sep="_"))
      
      # Ensure the directory exists
      if (!dir.exists(feather_folder_path)) {
        dir.create(feather_folder_path, recursive = TRUE)
      }
      
      # Write the combined feather file
      write_feather(feather_data, file.path(feather_folder_path, paste0(group_name, ".feather")))
      
    }, error = function(e) {
      log_error(paste("Failed to create or write feather file:", e$message),
                measurement_id = meas_ids[i],
                additional_info = paste("Group:", group_name))
    })
  }
  
  # Write all site datafiles after processing all subseries
  for(site_name in sites_in_series) {
    tryCatch({
      datafile_path <- file.path(main_folder, site_name)
      
      # In case the folder does not exist, create it
      if (!dir.exists(datafile_path)) {
        dir.create(datafile_path, recursive = TRUE)
      }
      write.csv(site_datafiles[[site_name]], file.path(datafile_path, paste0(site_name, "whole.csv")), row.names = FALSE)
      cat(paste("Written datafile for site:", site_name, "\n"))
    }, error = function(e) {
      log_error(paste("Failed to write datafile:", e$message),
                measurement_id = meas_ids[i], site_id = site_name)
    })
  }
  
} # closing i loop (iterating over measurements)

# OVERALL DATA HIERARCHY SUMMARY:
# 1. MEASUREMENT ID: Data collection session (outer loop i)
# 2. SUBSERIES: Individual gas measurement from one instrument (middle loop j) 
# 3. SITE: Location where measurements taken (inner loop k)
# 4. GROUP: Instrument session combining multiple gases (feather file grouping)
#    - Groups subseries by: date + subsiteid + point + point_type + start_time + end_time
#    - Creates combined feather files with separate columns per gas type
#    - Each group represents one measurement session (max 3 gases: co2, ch4, n2o)

##### combining folders with similar names but with spaces (as well as the summary files)

library(fs)
library(readr)
library(dplyr)

# Rename "Zwolse bos" to "Zwolse Bos" in the specified path
zwolse_path <- "../Holisoils_GHG_data/Zwolse bos"
zwolse_new_path <- "../Holisoils_GHG_data/Zwolse Bos"

if (dir_exists(zwolse_path)) {
  tryCatch({
    file.rename(zwolse_path, zwolse_new_path)
  }, error = function(e) {
    log_error(paste("Failed to rename Zwolse bos folder:", e$message),
              additional_info = paste("From:", zwolse_path, "To:", zwolse_new_path))
  })
}


# Get all 1st-level subfolders in the current working directory
subfolders <- dir_ls("../Holisoils_GHG_data", type = "directory")

# Identify folders with and without spaces
folders_with_space <- subfolders[grepl(" $", subfolders)]  # Detects folders ending with a space
folders_without_space <- gsub(" $", "", folders_with_space)  # Removes only the trailing space

# Process each folder pair
for (i in seq_along(folders_with_space)) {
  folder_space <- folders_with_space[i]
  folder_no_space <- folders_without_space[i]
  
  # Check if the corresponding folder without space exists
  if (folder_no_space %in% subfolders) {
    
    tryCatch({
      # Move all 2nd-level subfolders
      second_level_folders <- dir_ls(folder_space, type = "directory")
      for (subfolder in second_level_folders) {
        new_path <- file.path(folder_no_space, basename(subfolder))
        file_move(subfolder, new_path)
      }
    }, error = function(e) {
      log_error(paste("Failed to move subfolders:", e$message),
                additional_info = paste("From:", folder_space, "To:", folder_no_space))
    })
    
    # Identify summary CSV files
    folder_name_space <- basename(folder_space)
    folder_name_no_space <- basename(folder_no_space)
    
    summary_file_space <- file.path(folder_space, paste0(folder_name_space, "_combined.csv"))
    summary_file_no_space <- file.path(folder_no_space, paste0(folder_name_no_space, "_combined.csv"))
    
    # Merge summary CSV files if both exist
    if (file_exists(summary_file_space) && file_exists(summary_file_no_space)) {
      tryCatch({
        df_space <- read_csv(summary_file_space, show_col_types = FALSE)
        df_no_space <- read_csv(summary_file_no_space, show_col_types = FALSE)
        
        df_combined <- bind_rows(df_no_space, df_space)
        
        # Write the combined summary to the folder without spaces
        write_csv(df_combined, summary_file_no_space)
      }, error = function(e) {
        log_error(paste("Failed to merge _combined.csv files:", e$message),
                  additional_info = paste("Files:", summary_file_space, "and", summary_file_no_space))
      })
    }
    
    
    summary_file_space_db3like <- file.path(folder_space, paste0(folder_name_space, "_db3.csv"))
    summary_file_no_space_db3like <- file.path(folder_no_space, paste0(folder_name_no_space, "_db3.csv"))
    
    # Merge summary CSV files if both exist
    if (file_exists(summary_file_space_db3like) && file_exists(summary_file_no_space_db3like)) {
      tryCatch({
        df_space <- read_csv(summary_file_space_db3like, show_col_types = FALSE)
        df_no_space <- read_csv(summary_file_no_space_db3like, show_col_types = FALSE)
        
        df_combined <- bind_rows(df_no_space, df_space)
        
        # Write the combined summary to the folder without spaces
        write_csv(df_combined, summary_file_no_space_db3like)
      }, error = function(e) {
        log_error(paste("Failed to merge _db3.csv files:", e$message),
                  additional_info = paste("Files:", summary_file_space_db3like, "and", summary_file_no_space_db3like))
      })
    }
    
    tryCatch({
      dir_delete(folder_space)
    }, error = function(e) {
      log_error(paste("Failed to delete folder with space:", e$message),
                additional_info = paste("Folder:", folder_space))
    })
    
  }
}

# Final log entry
cat("\n=== Processing completed ===", "\n", file = log_file, append = TRUE)
cat("Finished:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n", file = log_file, append = TRUE)