

library(readxl)

Gamiz_new <- read_excel("./Data_to_patch/Spain/Gamiz_LPI_LUKE.xlsx")

Gamiz_old <- read.csv("./Holisoils_GHG_data/Gamiz/Gamizwhole.csv")



library(dplyr)
library(tidyr)


# Function to convert new dataset structure to match old dataset structure
convert_new_to_old_structure <- function(df_new) {
  
  # First, let's see which gases were actually measured
  gases_measured <- c()
  if(any(df_new$co2 == 1)) gases_measured <- c(gases_measured, "co2")
  if(any(df_new$ch4 == 1)) gases_measured <- c(gases_measured, "ch4") 
  if(any(df_new$n2o == 1)) gases_measured <- c(gases_measured, "n2o")
  
  print(paste("Gases measured:", paste(gases_measured, collapse = ", ")))
  
  # Create a list to store rows for each gas
  gas_rows <- list()
  
  for(gas in gases_measured) {
    # Filter rows where this gas was measured
    gas_data <- df_new[df_new[[gas]] == 1, ]
    
    # Add gas column
    gas_data$gas <- gas
    
    # Map flux values based on gas type
    if(gas == "co2") {
      # CO2 flux is in the 'respiration' column (not 'personal_flux')
      gas_data$autotrim_flux <- as.numeric(gas_data$respiration)
    } else if(gas == "ch4") {
      # CH4 flux is in the 'methane' column
      gas_data$autotrim_flux <- as.numeric(gas_data$methane)
    } else if(gas == "n2o") {
      # N2O flux - you might need to identify which column contains this
      gas_data$autotrim_flux <- NA  # Update this when you know the column
    }
    
    gas_rows[[gas]] <- gas_data
  }
  
  # Combine all gas rows
  result <- do.call(rbind, gas_rows)
  
  # Rename and map columns to match old structure
  result <- result %>%
    rename(
      meas = measurements,
      volume = chamber_vol,
      area = chamber_area,
      soil_temp_5cm = t05,
      soil_temp_30cm = t10
    ) %>%
    mutate(
      # Convert date to character to match old format
      date = as.character(date),
      # Extract time components from start_time and end_time
      start_time = format(start_time, "%H:%M:%S"),
      end_time = format(end_time, "%H:%M:%S"),
      # Convert character columns to appropriate types
      soil_temp_5cm = as.numeric(soil_temp_5cm),
      soil_temp_30cm = as.numeric(soil_temp_30cm),
      tsmoisture = as.numeric(tsmoisture),
      wt = as.logical(wt),
      # Add missing columns that exist in old dataset
      unit = "ppm",
      autotrim_resid = NA,
      personal_flux = NA,
      personal_resid = NA,
      trimmer = NA,
      db_origin = NA,
      pad_head = 0,
      pad_tail = 0,
      filename = NA
    )
  
  # Remove columns that don't exist in old structure
  columns_to_remove <- c("...1", "sitedesc", "chambersetting", "notes1", "notes2", "notes3", 
                         "fabric", "weather", "wind", "start_ppm", "end_ppm", 
                         "chamber_vol_II", "t05_egm", "tsm_egm", "empty", "co2", "ch4", "n2o",
                         "unique_id", "file_id", "subfolder", "device", "chamber", 
                         "respiration", "methane", "sample_name")
  
  # Only remove columns that actually exist in the dataset
  columns_to_remove <- columns_to_remove[columns_to_remove %in% names(result)]
  result <- result %>% select(-all_of(columns_to_remove))
  
  # Reorder columns to match old dataset structure (based on the str() output)
  desired_order <- c("id", "meas", "date", "siteid", "subsiteid", "point", "gas", 
                     "start_time", "end_time", "start_temp", "end_temp", "unit", "wt", 
                     "pointtype", "soil_temp_5cm", "soil_temp_30cm", "tsmoisture", 
                     "volume", "area", "pad_head", "pad_tail", "autotrim_flux", 
                     "autotrim_resid", "personal_flux", "personal_resid", "trimmer", 
                     "filename", "db_origin")
  
  # Only reorder columns that exist in both the desired order and the result
  existing_columns <- intersect(desired_order, names(result))
  remaining_columns <- setdiff(names(result), existing_columns)
  
  result <- result %>% select(all_of(c(existing_columns, remaining_columns)))
  
  return(result)
}

# Apply the conversion
Gamiz_new_converted <- convert_new_to_old_structure(Gamiz_new)

# Check the structure of the converted data
str(Gamiz_new_converted)

# Compare with old data structure
print("Old data structure:")
str(Gamiz_old)

print("New converted data structure:")
str(Gamiz_new_converted)


# Convert date formats to match for comparison
Gamiz_old$date <- as.Date(Gamiz_old$date)
Gamiz_new_converted$date <- as.Date(Gamiz_new_converted$date)

# Get column names from both datasets
old_cols <- colnames(Gamiz_old)
new_cols <- colnames(Gamiz_new_converted)

# Check which old columns are missing in new (accounting for name changes)
missing_cols <- setdiff(old_cols, new_cols)
print("Columns in old dataset not found in new dataset:")
print(missing_cols)

Gamiz_new_converted$meas
Gamiz_old$meas

plotrange = range(as.Date(Gamiz_old$date), as.Date(Gamiz_new_converted$date))
plot(as.Date(Gamiz_new_converted$date), Gamiz_new_converted$autotrim_flux,  type = "p", col = "blue", pch = 19,
     xlab = "Date", ylab = "CO2 Flux", main = "CO2 Flux Over Time (New Dataset)", xlim = plotrange)
points(as.Date(Gamiz_old$date), Gamiz_old$autotrim_flux, col = "red", pch = 19)




# Get the column names from the old dataset (this is our target structure)
old_cols <- colnames(Gamiz_old)
print("Old dataset structure (to be preserved):")
print(old_cols)

# Select only the columns from new dataset that exist in old dataset
Gamiz_new_subset <- Gamiz_new_converted[, colnames(Gamiz_new_converted) %in% old_cols]

# Ensure the column order matches the old dataset exactly
Gamiz_new_subset <- Gamiz_new_subset[, old_cols[old_cols %in% colnames(Gamiz_new_subset)]]

# Add missing columns to new dataset only (filled with NA) to match old structure
missing_cols <- setdiff(old_cols, colnames(Gamiz_new_subset))
print("Columns missing in new dataset (will be filled with NA):")
print(missing_cols)

for(col in missing_cols) {
  Gamiz_new_subset[[col]] <- NA
}

# Reorder columns to exactly match old dataset
Gamiz_new_subset <- Gamiz_new_subset[, old_cols]

# Convert date formats to match
Gamiz_old$date <- as.Date(Gamiz_old$date)
Gamiz_new_subset$date <- as.Date(Gamiz_new_subset$date)

# Show date ranges to confirm they're different periods
print("Date ranges:")
print(paste("Old dataset:", min(Gamiz_old$date, na.rm=T), "to", max(Gamiz_old$date, na.rm=T)))
print(paste("New dataset:", min(Gamiz_new_subset$date, na.rm=T), "to", max(Gamiz_new_subset$date, na.rm=T)))

# Combine the datasets
Gamiz_combined <- rbind(Gamiz_old, Gamiz_new_subset)

# Report results
print("Combination results:")
print(paste("Old dataset rows:", nrow(Gamiz_old)))
print(paste("New dataset rows:", nrow(Gamiz_new_subset)))
print(paste("Combined dataset rows:", nrow(Gamiz_combined)))
print(paste("Combined date range:", min(Gamiz_combined$date, na.rm=T), "to", max(Gamiz_combined$date, na.rm=T)))

# Verify structure is preserved
print("Final structure matches old dataset:")
print(identical(colnames(Gamiz_combined), colnames(Gamiz_old)))


points(as.Date(Gamiz_combined$date), Gamiz_combined$autotrim_flux, col = "black", pch = 1)

# Save the combined dataset
write.csv(Gamiz_old, "./Holisoils_GHG_data/Gamiz/Gamiz_before_newdata.csv", row.names = FALSE)
write.csv(Gamiz_combined, "./Holisoils_GHG_data/Gamiz/Gamizwhole.csv", row.names = FALSE)

