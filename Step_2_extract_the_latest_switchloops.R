

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
for(i in 1:length(meas_ids)){
  
  cat("Processing measurement ID", i, "of", length(meas_ids), "- ID:", meas_ids[i], "\n")
  cat("Downloading...", "\n")
  
  tic() # bit of benchmarking...
  s0 <- get_series(tok, meas_ids[i])
  toc()
  
  # series summary in tabular form, loop should run for each row
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
  
  # Which sites were measured in the current series?
  sites_in_series <- unique(series_table$siteid)
  
  # Initialize site datafiles storage for this measurement
  site_datafiles <- list()
  
  # Pre-initialize all site datafiles for this measurement
  for(site_name in sites_in_series) {
    # Define paths immediately after site_name
    datafile_path <- file.path(main_folder, site_name)
    datafile_path_file <- file.path(main_folder, site_name, site_name)
    
    # Read if the datafile already exists
    if(file.exists(file.path(datafile_path, paste0(site_name, "whole.csv")))) {
      site_datafile <- read.csv(file.path(datafile_path, paste0(site_name, "whole.csv")))
      
      # Add missing treatment column if it doesn't exist
      if(!"treatment" %in% names(site_datafile)) {
        site_datafile$treatment <- NA_character_
        cat("    Added missing 'treatment' column for", site_name, "\n")
      }
      
      # Add missing filename column if it doesn't exist
      if(!"filename" %in% names(site_datafile)) {
        site_datafile$filename <- NA_character_
        cat("    Added missing 'filename' column for", site_name, "\n")
      }
      
      # Convert to tibble
      site_datafile <- as_tibble(site_datafile)
      
    } else {
      cat(paste("Initialised file for", site_name, "not found, initialising data structure", '\n'))
      
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
      cat(paste("combining the initialised data structure with the older sources for", site_name, '\n'))
      
      # First step: add the old data in the data structure for current site
      if(file.exists(paste(datafile_path_file, "db3like.csv", sep=""))) {
        old_datafile <- read.csv(paste(datafile_path_file, "db3like.csv", sep=""))[,-1]
        site_datafile$db_origin <- 3
        combined_datafile <- rbind(old_datafile, site_datafile)
      } else {
        combined_datafile <- site_datafile
      }
    } # end of the IF loop for combining the data files
    
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
  
  # Middle loop - running for each subseries across all sites  
  for(j in 1:total_subseries){
    
    # Process the current subseries
    s_obj <- process_single_series(s0, j)
    current_site <- s_obj$siteid
    
    cat(paste("    Processing subseries", j, "for site", current_site, "\n"))
    
    s_id <- s_obj$id
    f_obj <- get_flux(tok, s_id)
    
    # Process flux calculations
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
    }
    
    # Generate sequence of 1-second intervals
    start_time <- as.POSIXct(paste(s_obj$date, s_obj$start_time), format = "%Y-%m-%d %H:%M:%S")
    end_time <- as.POSIXct(paste(s_obj$date, s_obj$end_time), format = "%Y-%m-%d %H:%M:%S")
    time_sequence <- seq(from = start_time, to = end_time, by = "1 sec")
    time_sequence_formatted <- format(time_sequence, "%H:%M:%S")
    trimmed_values <- s_obj$values[(s_obj$pad_head + 1):(length(s_obj$values) - s_obj$pad_tail)]
    
    # Make the generated timesteps and the measurement vectors uniform
    if (length(trimmed_values) - length(time_sequence_formatted) > 0){
      cat(paste("Site", current_site, "series", s_obj$id, "subseries", j, ": measurements", 
                length(trimmed_values) - length(time_sequence_formatted), "shorter than time vector", '\n'))
      diff_vecs[i] = length(trimmed_values) - length(time_sequence_formatted)
      trimmed_values = trimmed_values[(1 + diff_vecs[i]):length(trimmed_values)]
    } else if (length(trimmed_values) - length(time_sequence_formatted) < 0) {
      cat(paste("Site", current_site, "series", s_obj$id, "subseries", j, ": measurements", 
                abs(length(trimmed_values) - length(time_sequence_formatted)), "longer than time vector", '\n'))
      diff_vecs[i] = length(time_sequence_formatted) - length(trimmed_values)
      time_sequence_formatted = time_sequence_formatted[(1 + diff_vecs[i]):length(time_sequence_formatted)]
    }
    
    # Create the name of the file to write
    filename_featherfile = paste(s_obj$date,
                                 s_obj$subsiteid,
                                 s_obj$point,
                                 s_obj$env$`point type`, sep="_")
    
    # Clean up filename if it contains problematic characters
    if (grepl("/", filename_featherfile)) {
      filename_featherfile <- gsub("/", "-", filename_featherfile)
    }
    
    # Inner loop - now we process each site that this subseries affects
    # (In most cases, this will just be one site, but keeping the loop structure)
    for(k in which(sites_in_series == current_site)){
      
      site_name <- sites_in_series[k]
      
      cat(paste("      Processing for site", site_name, "(site", k, "of", length(sites_in_series), ")\n"))
      
      # Define paths
      datafile_path <- file.path(main_folder, site_name)
      
      # Add row to the appropriate site datafile
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
      
      # Create the feather file to write
      if(length(trimmed_values) == 0){
        featherfile = data.frame(series = NA,
                                 time = NA,
                                 flux = NA,
                                 db_origin = 3)
        cat(paste("The measurement series is empty, filled as NA", '\n'))
      } else {
        featherfile = data.frame(series = rep(s_obj$id, length(trimmed_values)),
                                 time = time_sequence_formatted,
                                 flux = trimmed_values,
                                 db_origin = 3)
      }
      
      # Create the full feather file path
      feather_folder_path <- file.path(datafile_path, paste("meas_ID", meas_ids[i], "db3", sep="_"))
      
      # Ensure the directory exists
      if (!dir.exists(feather_folder_path)) {
        dir.create(feather_folder_path, recursive = TRUE)
      }
      write_feather(featherfile, paste(feather_folder_path, "/", filename_featherfile, ".feather", sep=""))
      
    } # closing k loop (now innermost - processing sites for current subseries)
    
    pb$tick()
    
  } # closing j loop (now middle - running for each subseries across all sites)
  
  # Write all site datafiles after processing all subseries
  for(site_name in sites_in_series) {
    datafile_path <- file.path(main_folder, site_name)
    
    # In case the folder does not exist, create it
    if (!dir.exists(datafile_path)) {
      dir.create(datafile_path, recursive = TRUE)
    }
    write.csv(site_datafiles[[site_name]], file.path(datafile_path, paste0(site_name, "whole.csv")), row.names = FALSE)
    cat(paste("Written datafile for site:", site_name, "\n"))
  }
  
} # closing i loop (iterating over measurements)




##### combining folders with similar names but with spaces (as well as the summary files)

library(fs)
library(readr)
library(dplyr)

# Rename "Zwolse bos" to "Zwolse Bos" in the specified path
zwolse_path <- "../Holisoils_GHG_data/Zwolse bos"
zwolse_new_path <- "../Holisoils_GHG_data/Zwolse Bos"

if (dir_exists(zwolse_path)) {
  file.rename(zwolse_path, zwolse_new_path)
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
    
    # Move all 2nd-level subfolders
    second_level_folders <- dir_ls(folder_space, type = "directory")
    for (subfolder in second_level_folders) {
      new_path <- file.path(folder_no_space, basename(subfolder))
      file_move(subfolder, new_path)
    }
    
    # Identify summary CSV files
    folder_name_space <- basename(folder_space)
    folder_name_no_space <- basename(folder_no_space)
    
    summary_file_space <- file.path(folder_space, paste0(folder_name_space, "_combined.csv"))
    summary_file_no_space <- file.path(folder_no_space, paste0(folder_name_no_space, "_combined.csv"))
    
    # Merge summary CSV files if both exist
    if (file_exists(summary_file_space) && file_exists(summary_file_no_space)) {
      df_space <- read_csv(summary_file_space, show_col_types = FALSE)
      df_no_space <- read_csv(summary_file_no_space, show_col_types = FALSE)
      
      df_combined <- bind_rows(df_no_space, df_space)
      
      # Write the combined summary to the folder without spaces
      write_csv(df_combined, summary_file_no_space)
    }
    
    
    summary_file_space_db3like <- file.path(folder_space, paste0(folder_name_space, "_db3.csv"))
    summary_file_no_space_db3like <- file.path(folder_no_space, paste0(folder_name_no_space, "_db3.csv"))
    
    # Merge summary CSV files if both exist
    if (file_exists(summary_file_space_db3like) && file_exists(summary_file_no_space_db3like)) {
      df_space <- read_csv(summary_file_space_db3like, show_col_types = FALSE)
      df_no_space <- read_csv(summary_file_no_space_db3like, show_col_types = FALSE)
      
      df_combined <- bind_rows(df_no_space, df_space)
      
      # Write the combined summary to the folder without spaces
      write_csv(df_combined, summary_file_no_space_db3like)
    }
    
    dir_delete(folder_space)
    
  }
}

