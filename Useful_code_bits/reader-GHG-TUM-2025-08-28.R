## 2025-08-13

library(tidyverse)
library(stringr) # str_split
library(readxl)

## test reading a single file at a time
## TODO: loop through all directories, files, etc.

directories <- c("KelM_2022","KelM_2023","KelS_2022","KelS_2023","KraS_2022",
                 "KraS_2023","KraS_2024","WasM_2022","WasM_2023","WasS_2022",
                 "WasS_2023")

path <- paste("GHG_TUM_forJani_2025_08_12/",directories[1],sep="")

files <- dir(path)
stems <- unique(substr(files,1,13))

df0 <- file(paste(path,"/",stems[1],".csv",sep="")) # these are NOT csv files

df <- readLines(df0); close(df0)

## these df rows should contain the point identifiers
file_rows <- which(substr(df,1,9)=="File Name")
point_ids0 <- unlist(str_split(df[file_rows],'\t'))
point_ids <- point_ids0[c(FALSE,TRUE)] # remove every odd element

## these rows precede and follow all parts with actual data
type_rows <- which(substr(df,1,4)=="Type")
gcid_rows <- which(substr(df,1,11)=="GasColumnID")

if(length(type_rows) != length(gcid_rows)){ cat("error: rows mismatch\n") } #-> stopifnot()

size <- length(type_rows)

## read in the field form

ff0 <- read_excel(paste(path,"/",stems[1],"_ghg_field_form.xlsx",sep=""))
ff <- ff0 %>% drop_na("Start time") %>% filter(!row_number() == 1) %>%
    rename(date = "Date (yyyy-mm-dd)",
           siteid = "Monitoring site",
           subsite = "Sub-site ID",
           point = "Monitoring point ID",
           start_time = "Start time",
           end_time = "End time",
           chamber_start_t = "Chamber start T, C",
           chamber_end_t = "Chamber end T, C",
           t05 = "T at 05, C",
           soil_moisture = "Soil moisture at topsoil",
           notes = "Notes_1",
           area = "Chamber area, dm2",
           volume = "Chamber volume, dm3",
           point_type = "Monitoring point type")

## the ff rows and df series are in matching order iff point_ids is identical to ff$point
if(!all(point_ids == ff$point)){ cat("error: different order\n") } #-> stopifnot()

## test extracting df contents one series at a time
## TODO: loop through all parts
row <- 1

header <- str_split(df[type_rows[row]],'\t')
dfsub <- df[(type_rows[row]+1):(gcid_rows[row]-1)]

d0 <- tibble(date=Date(),time=character(),temp=numeric(),co2=numeric())
for(i in 1:length(dfsub)){
    split0 <- (str_split(dfsub[i],'\t'))[[1]]
    type <- as.numeric(split0[1]) ## only collect lines where type equals 1
    if(type == 1){
        d0 <- d0 %>% add_row(date = date(ymd_hms(split0[3])),
                             time = substr(as.character(split0[3]),12,19),
                             temp = as.numeric(split0[4]),
                             co2 = as.numeric(split0[7]))
    }
}
d0$period <- hms(d0$time)
d0$psec <- period_to_seconds(d0$period)
d0$s <- d0$psec - min(d0$psec)

plot(d0$s,d0$co2)

## find optimal trim by trying all trims between 0 and 30 from both head and tail
## using confidence interval as criterion
## note 1: this criterion turns out to be quite conservative, personally I would
## sometimes trim a bit more points out
## note 2: this code here just finds the best trim, and will not identify
## failed measurements, and problems such as clock mismatch could easily go unnoticed

## you could go bigger than 30 of course, but have to ensure that the remaining
## number of datapoints is big enough

max_head_trim <- 30
max_tail_trim <- 30
if((nrow(d0) - max_head_trim - max_tail_trim) < 30){
    cat("error: not enough points to find best trim")
}

## this loop is rather slow, maybe there is a better way in R than this
## personally I would rather do something like this in Julia than R
trims <- tibble(head=integer(),tail=integer(),ci=double())
for(i in 0:max_head_trim){
    for(j in 0:max_tail_trim){
        d1 <- d0[(1+i):(nrow(d0)-j),] %>% select(s,co2)
        lmfit <- lm(co2 ~ s, data=d1)
        ci_value <- ((confint(lmfit))[2,2] - (confint(lmfit))[2,1])/2.0
        trims <- trims %>% add_row(head = i,
                                   tail = j,
                                   ci = ci_value)
    }
}
best_trim <- which(trims$ci == min(trims$ci))
best_head <- trims$head[best_trim]
best_tail <- trims$tail[best_trim]

d1 <- d0[(1+best_head):(nrow(d0)-best_tail),] %>% select(s,co2)
plot(d0$s,d0$co2)
abline(v=best_head,lty=3); abline(v=nrow(d0)-best_tail,lty=3)
points(d1$s,d1$co2,col='red',pch=16,cex=1.0)
lmfit <- lm(co2 ~ s, data=d1)
abline(lmfit)

