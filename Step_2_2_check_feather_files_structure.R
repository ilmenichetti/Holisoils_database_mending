library(arrow)

#take one feather file from the first 2 databases
db1_feather_test_file = read_feather("./Holisoils_GHG_data/Dobroc/613_db1/2021-08-25_DP-MC_C1_Trenched.feather", col_select = NULL, as_data_frame = TRUE, mmap = TRUE)


#take one feather file from the last database iteration
db2_feather_test_file = read_feather("./Holisoils_GHG_data/Dobroc/meas_ID_50_db3/2023-03-21_DP-MC_C1_Trenched.feather", col_select = NULL, as_data_frame = TRUE, mmap = TRUE)


str(db1_feather_test_file)
str(db2_feather_test_file)
