

# Get all subfolder names 
subfolders <- list.dirs(path = "./Holisoils_GHG_data/.", full.names = FALSE, recursive = FALSE)

# Initialize an empty list to store data frames
all_data <- list()

# Loop through each subfolder
for (i in subfolders) {
  # Construct the file path
  file_path <- file.path("./Holisoils_GHG_data", i, paste0(i, "moisturefilled.csv"))
  file_path_olddb <- file.path("./Holisoils_GHG_data",i, paste0(i, "db3like.csv"))
  
  # Check if the file exists before reading
  if (file.exists(file_path)) {
    # Read the CSV file
    temp_data <- read.csv(file_path)
    
    #in case the file is new only
    if (!"db_origin" %in% colnames(temp_data)) {
      temp_data$db_origin = 3
    }
    
    # Append to the list
    all_data[[i]] <- temp_data
  } else {
    #in case the file is old only
    
    message(paste("Combined file not found:", file_path, "proceeding with the old db version"))
    # Append to the list
    temp_data <- read.csv(file_path_olddb)
    temp_data$db_origin = 1
    # Append to the list
    all_data[[i]] <- temp_data
  }
}

# Combine all data frames into a single data frame

# Extract column names from each data frame in all_data
colnames_list <- lapply(all_data, colnames)

# Find the most common set of column names (assuming there is a dominant structure)
common_names <- Reduce(intersect, colnames_list)

# Identify data frames that do NOT match the common names
mismatched <- which(sapply(colnames_list, function(x) !all(common_names %in% x)))

# Print the indices and column names of the mismatched data frames
if (length(mismatched) > 0) {
  for (i in mismatched) {
    cat("Data frame at index", i, "has different columns:\n")
    print(setdiff(colnames_list[[i]], common_names))  # Shows extra columns
    print(setdiff(common_names, colnames_list[[i]]))  # Shows missing columns
    cat("\n")
  }
} else {
  cat("All data frames have matching column names.\n")
}

attributes_list <- lapply(all_data, attributes)

# Print unique attributes across data frames
unique(attributes_list)

# Check for trailing spaces or encoding issues
colnames_check <- unique(lapply(all_data, function(df) trimws(colnames(df))))
print(colnames_check)

all_data <- lapply(all_data, function(df) {
  if (colnames(df)[1] %in% c("X", "...1")) {
    df <- df[, -1]  # Remove first column
  }
  df
})



#final_data <- do.call(rbind, all_data)

final_data <- NULL  # Initialize an empty object to store results

for (i in seq_along(all_data)) {
  cat("Processing element:", i, "\n")  # Print progress
  
  # Check if the current element is a data frame
  if (!is.data.frame(all_data[[i]])) {
    cat("Error at element", i, "- not a data frame\n")
    next  # Skip to the next iteration if not a data frame
  }
  
  # Try to bind the current data frame and catch any errors
  tryCatch({
    final_data <- rbind(final_data, all_data[[i]])
  }, error = function(e) {
    cat("Error at element", i, ":", e$message, "\n")
    names(final_data)
    names(all_data[[i]])
    
  })
}

cat("Finished processing.\n")




flux_absent = is.na(final_data$personal_flux) & is.na(final_data$autotrim_flux)
length(which(flux_absent))
length((flux_absent))

final_data = cbind(final_data, flux_absent)
# Save the combined data frame to a CSV file
write.csv(final_data, "All_sites.csv", row.names = FALSE)

