
library(readxl)
library(dplyr) # For easier data manipulation

# Open a connection to a text file for the report
output_file <- "moisture_merge_report.txt"
# Create/overwrite the report file (make it empty)
file.create(output_file)
#con <- file(output_file, "w")  # Open file for writing

# Function to read and process a single Excel file
process_file <- function(file) {
  read_excel(file, skip = 1) # Skip the first row (header) and the second row
}

# Get all subfolder names 
subfolders <- list.dirs(path = "./Holisoils_GHG_data", full.names = FALSE, recursive = FALSE)



# Loop through each subfolder
counter <- 0
for (j in subfolders) {
  # Construct the file path
    file_path <- file.path("./Holisoils_GHG_data",j, paste0(j, "whole.csv"))

    # Read the CSV file
    if (file.exists(file_path)) {
      # Read the file if it exists
      site_data <- read.csv(file_path)
      cat("Combined file read successfully.\n")
    } else {
      file_path <- file.path("./Holisoils_GHG_data",j, paste0(j, "db3like.csv"))
      site_data <- read.csv(file_path)
      cat("Combined file doesn't exist, reading the db3like.\n")
    }
    
    #read all the field forms with the site name
    files <- list.files("../Moisture/hs-fieldforms-2024-01-10/", pattern = j, full.names = TRUE, ignore.case = TRUE)
    
    if (length(files) == 0) {
      no_files_message <- paste("No moisture files found for site:", j, "\n")
      cat(no_files_message, file = output_file, append = TRUE)
    }
    
    # Combine all files into a single data frame
    # Initialize an empty list to store the data frames
    data_list <- list()
    
    # Loop through each file and process it
    for (i in seq_along(files)) {
      cat("Processing file:", files[i], "\n") # Print the file being processed
      library(readxl)
      
      # First, read without headers to check actual number of columns
      temp_data <- read_excel(files[i], skip = 2, col_names = FALSE)
      actual_cols <- ncol(temp_data)
      expected_cols <- length(headers)
      
      cat("  File has", actual_cols, "columns, expected", expected_cols, "\n")
      
      if (actual_cols < expected_cols) {
        # File has fewer columns than expected headers
        headers_to_use <- headers[1:actual_cols]
        missing_headers <- headers[(actual_cols + 1):expected_cols]
        cat("  WARNING: Missing columns in file", basename(files[i]), ":\n")
        cat("    Missing:", paste(missing_headers, collapse = ", "), "\n")
        
        # Read with available headers
        data <- read_excel(files[i], skip = 2, col_names = headers_to_use)
        
        # Add missing columns as NA
        for (missing_col in missing_headers) {
          data[[missing_col]] <- NA
        }
        
      } else if (actual_cols > expected_cols) {
        # File has more columns than expected headers - discard extra columns
        cat("  WARNING: Extra columns in file", basename(files[i]), "- discarding", (actual_cols - expected_cols), "extra columns\n")
        
        # Read only the expected number of columns by specifying range
        data <- read_excel(files[i], skip = 2, col_names = headers, range = cell_cols(1:expected_cols))
        
      } else {
        # Perfect match
        cat("  Column count matches expected headers\n")
        data <- read_excel(files[i], skip = 2, col_names = headers)
      }
      
      # ENHANCED CLEANING: Remove header contamination that varies by file
      if ("Date (yyyy-mm-dd)" %in% names(data)) {
        before_cleaning <- nrow(data)
        
        # Remove NA rows
        data <- data[!is.na(data$`Date (yyyy-mm-dd)`), ]
        
        # Remove header contamination rows (these vary by file structure)
        header_contamination <- c("Date (yyyy-mm-dd)", "YYYY-MM-DD", "Date", "yyyy-mm-dd", 
                                  "Monitoring site", "Sub-site ID", "Monitoring point ID",
                                  "Droplist", "ID Code", "HH:MM:SS", "Value", "Formula")
        
        data <- data[!data$`Date (yyyy-mm-dd)` %in% header_contamination, ]
        
        # Keep only rows that look like actual dates (YYYY-MM-DD format)
        date_pattern <- "^\\d{4}-\\d{2}-\\d{2}$"
        data <- data[grepl(date_pattern, data$`Date (yyyy-mm-dd)`), ]
        
        after_cleaning <- nrow(data)
        removed_rows <- before_cleaning - after_cleaning
        
        if (removed_rows > 0) {
          cat("  Cleaned", removed_rows, "header/invalid rows, left with", after_cleaning, "data rows\n")
        }
        
      } else {
        cat("  WARNING: 'Date (yyyy-mm-dd)' column not found, using first column for filtering\n")
        data <- data[!is.na(data[[1]]), ]
      }
      
      data_list[[i]] <- data                 # Store the processed data in the list
      
      # Check time columns with more flexible column detection
      end_time_col <- NULL
      start_time_col <- NULL
      
      # Find time columns (they might be named differently)
      for (col_name in names(data)) {
        if (grepl("end.*time", col_name, ignore.case = TRUE)) {
          end_time_col <- col_name
        }
        if (grepl("start.*time", col_name, ignore.case = TRUE)) {
          start_time_col <- col_name
        }
      }
      
      # Check time formats
      if (!is.null(end_time_col) && !inherits(data[[end_time_col]], "POSIXt")) {
        cat("  WARNING: End time column in", basename(files[i]), "is not in POSIXt format\n")
        cat("  End time column '", end_time_col, "' has class:", class(data[[end_time_col]]), "\n")
        # Don't break, just warn
      }
      
      if (!is.null(start_time_col) && !inherits(data[[start_time_col]], "POSIXt")) {
        cat("  WARNING: Start time column in", basename(files[i]), "is not in POSIXt format\n")
        cat("  Start time column '", start_time_col, "' has class:", class(data[[start_time_col]]), "\n")
        # Don't break, just warn
      }
      
      cat("  Successfully processed", nrow(data), "rows\n")
    }

    
        # Initialize an empty data frame for the combined data
    combined_data <- NULL
    
    # Combine data frames from the list using a loop
    for (i in seq_along(data_list)) {
      combined_data <- rbind(combined_data, data_list[[i]])
    }
    
    #modify the dates and times to work later
    combined_data$`Date (yyyy-mm-dd)` <- as.Date(combined_data$`Date (yyyy-mm-dd)`)

    # Safe time conversion - invalid times become NA
    combined_data$`Start time` <- tryCatch({
      hms::as_hms(combined_data$`Start time`)
    }, error = function(e) {
      cat("  Some Start time values invalid, converting individually\n")
      sapply(combined_data$`Start time`, function(x) {
        tryCatch(hms::as_hms(x), error = function(e) NA)
      })
    })
    
    combined_data$`End time` <- tryCatch({
      hms::as_hms(combined_data$`End time`)
    }, error = function(e) {
      cat("  Some End time values invalid, converting individually\n")
      sapply(combined_data$`End time`, function(x) {
        tryCatch(hms::as_hms(x), error = function(e) NA)
      })
    })
    
    moist_before = site_data$tsmoisture
    
    
    #trying to find the moisture to associate
    for(k in 1:dim(site_data)[1]){
        if(is.na(site_data[k,]$tsmoisture)){
          if(!is.null(dim(combined_data))){ #if there are some new moisture data to read
            site_date = as.Date(site_data[k,]$date)
            site_time <- hms::as_hms(site_data[k,]$start_time)
            
            #look into the assembled object to screen for the WT measurement
            filtered_data <- combined_data[combined_data$`Date (yyyy-mm-dd)` == site_date, ]
            filtered_data <- filtered_data[filtered_data$`Start time` == site_time, ]
            
            if(dim(filtered_data)[1] ==1){
              site_data[k,]$tsmoisture = filtered_data$`Soil moisture at topsoil`
            }
          } else {
            message2 <- paste("The site", j, "does not have any additional moisture data")
          }
          
        }
    }
    
    #writing the moisture filled version of each file
    new_file_path <- sub("whole", "moisturefilled", file_path)
    write.csv(site_data, new_file_path, row.names = FALSE)
    
    
    filled_moistures = length(which(is.na(moist_before))) - length(which(is.na(site_data$tsmoisture)))

    # Prepare the message
    message <- paste("In site:", j,
                     "I could fill", filled_moistures,
                     "elements of", length(which(is.na(moist_before))),
                     "\n")

    if(is.null(dim(combined_data))){ #if there are some new moisture data to read
      message = paste(message, message2, "\n")}
    
    # Write the message to the file
    #cat(message, file = con, append = TRUE)
    cat(message, file = output_file, append = TRUE)
    counter <- counter + 1
    
     
    
}

#close(con)
