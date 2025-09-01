
library(readxl)

# Get all subdirectories 
data_path = "Data_to_patch"
subfolders <- list.dirs(file.path(data_path, "Germany"), full.names = FALSE, recursive = FALSE)
subfolders <- subfolders[subfolders != ""]

# -------------------------------------------------------------------------------------
# Function to extract couple names from files, to be used in the following loop for each subfolder
# -------------------------------------------------------------------------------------
extract_couple_names <- function(files) {
  if (length(files) == 0) {
    return(character(0))
  }
  
  # Remove file extensions first
  files_no_ext <- tools::file_path_sans_ext(files)
  
  # Find the longest common prefixes that appear exactly twice
  max_length <- min(nchar(files_no_ext))
  couple_names <- c()
  
  for (prefix_length in max_length:1) {
    # Extract prefixes of current length
    prefixes <- substr(files_no_ext, 1, prefix_length)
    
    # Count how many files have each prefix
    prefix_counts <- table(prefixes)
    
    # Check if we have mostly pairs (couples)
    if (all(prefix_counts %in% c(2)) && length(prefix_counts) > 0) {
      couple_names <- names(prefix_counts)
      break
    }
  }
  
  if (length(couple_names) == 0) {
    stop(paste("Cannot determine file couples in files:", paste(files, collapse=", ")))
  }
  
  return(couple_names)
}
# ------------------------------------------------------------------------------------- end of function



########################################################################################
###################### LOOP PROCESSING EACH FOLDER ###################### 
########################################################################################

for (i in seq_along(subfolders)) {
  
  subfolder <- subfolders[i]
  
  tryCatch({
    # Get full path to current subfolder
    current_path <- file.path(data_path, "Germany", subfolder)
    
    # Get all files in current subfolder
    files <- list.files(current_path, full.names = FALSE)
    
    # Extract couple names for THIS subfolder
    couple_names <- extract_couple_names(files)
    
    # Now you have 'couple_names' for this specific subfolder
    cat("Subfolder:", subfolder, "\n")
    cat("Measurents found:", paste(couple_names, collapse=", "), "\n\n")
    
    # -------------------------------------------------------------------------------------
    # MAIN LOGIC OF THE PROCESSING
    # -------------------------------------------------------------------------------------
    
    for(j in seq_along(couple_names)){
      
      couple_name_current = couple_names[j]
      
      ##### file names
      
      # field form file of the couple
      ff_file <- list.files(current_path, pattern = paste0("^", couple_name_current, ".*_ghg_field_form\\.xlsx$"), full.names = TRUE, ignore.case = TRUE)
      
      # measurement file of the couple
      meas_file <- list.files(current_path, pattern = paste0("^", couple_name_current, ".*\\.csv$"), full.names = TRUE, ignore.case = TRUE)
      
      if(length(ff_file) == 0){
        cat("No field form file found for couple:", couple_name_current, "in subfolder:", subfolder, "\n")
        next
      }
      if(length(meas_file) == 0){
        cat("No measurement file found for couple:", couple_name_current, "in subfolder:", subfolder, "\n")
        next
      }
      
      ##### read files
      
      ff_table <- read_excel(ff_file)
      meas_table <- read.csv(meas_file)
      
      #data are between Labels_01 and GasColumnID
      #discard everything is not type 1
      
      
      
    }
    
    
    
    # ------------------------------------------------------------------------------------- 
    # end of the processing, from here on just closing the loop for folders
    # ------------------------------------------------------------------------------------- 
    
    
  }, error = function(e) {
    cat("Error processing", subfolder, ":", e$message, "\n\n")
  })
}
