# Filter GTFS for bounding box

# libraries
library(tidyverse)
# if (!require(devtools)) {
#   install.packages('devtools')
# }
# devtools::install_github('ropensci/gtfsr')
library(gtfsr)
library(stringi)


# load gtfs
oepnv <- import_gtfs(paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/20211022_fahrplaene_gesamtdeutschland_gtfs.zip"), local = TRUE)

# Split txt files into separate dfs

#mandatory
agency <- oepnv$agency_df
routes <- oepnv$routes_df
trips <- oepnv$trips_df
stop_times <- oepnv$stop_times_df
stops <- oepnv$stops_df

#optional - that's why we include an if statement
if (is.data.frame(oepnv$calendar_dates_df)) {calendar_dates <- oepnv$calendar_dates_df}
if (is.data.frame(oepnv$calendar_df)) {calendar <- oepnv$calendar_df}
if (is.data.frame(oepnv$feed_info_df)) {feed_info <- oepnv$feed_info_df}
if (is.data.frame(oepnv$shapes_df)) {shapes <- oepnv$shapes_df}


# define bounding box (e.g. at http://bboxfinder.com/#11.243957,47.966225,11.980041,48.292362)
# EMM+50km: 9.8053665,47.0885227,13.417136,49.3811241 


bbox = c(9.8053665,47.0885227,13.417136,49.3811241)

# filter step by step through the data

stops %>% filter(between(stop_lon, bbox[1], bbox[3]) & between(stop_lat, bbox[2], bbox[4]) ) -> stops

stop_times %>% filter(stop_id %in% stops$stop_id) -> stop_times

trips %>% filter(trip_id %in% stop_times$trip_id) -> trips

routes %>% filter(route_id %in% trips$route_id) -> routes

if (is.data.frame(oepnv$calendar_dates_df)) {
  calendar_dates %>% filter(service_id %in% trips$service_id) -> calendar_dates }

if (is.data.frame(oepnv$calendar_df)) {
  calendar %>% filter(service_id %in% trips$service_id) -> calendar }

if (is.data.frame(oepnv$shapes_df)) {
  shapes %>% filter(between(shape_pt_lon, bbox[1], bbox[3]) & between(shape_pt_lat, bbox[2], bbox[4]) ) -> shapes }  


# change calendar dates to make the analysis work until 2025 (loosing some accuracy here!)
calendar %>% mutate(start_date = "20200101") %>%  mutate(end_date = "20251231") -> calendar

# fix comma issue
agency$agency_name <-  str_replace(agency$agency_name, pattern = ",", replacement = "; ")
stop_times$stop_headsign <-  str_replace(stop_times$stop_headsign, pattern = ",", replacement = ";")
#export as txt

write.table(agency, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/agency.txt"), sep = ",", row.names = FALSE, quote = TRUE, qmethod = "double", fileEncoding = "UTF-8")
write.table(routes, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/routes.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8")
write.table(trips, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/trips.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8")
write.table(stop_times, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/stop_times.txt"), sep = ",", row.names = F, quote = T,qmethod = "double", fileEncoding = "UTF-8")
write.table(stops, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/stops.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8")
#write.table(transfers, "C:/Users/ga72jij/LRZ Sync+Share/DFG Erreichbarkeitsmodell Arbeitsstandorte/Daten/ÖV/gtfs.de/filtered_test/transfers.txt", sep=",", row.names = FALSE, quote = FALSE, qmethod = "double", fileEncoding = "UTF-8")

if (is.data.frame(oepnv$calendar_dates_df)) {
  write.table(calendar_dates,paste0(getwd(), "/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/calendar_dates.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8") }
if (is.data.frame(oepnv$calendar_df)) {
  write.table(calendar, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/calendar.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8") }
if (is.data.frame(oepnv$feed_info_df)) {
  write.table(feed_info, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/feed_info.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8") }
if (is.data.frame(oepnv$shapes_df)) {
  write.table(shapes, paste0(getwd(),"/r5r/data/delfi_emm_20211102_filtered-osm/EMM/2021-11-02/shapes.txt"), sep = ",", row.names = FALSE, quote = T, qmethod = "double", fileEncoding = "UTF-8") }


######### then: create ZIP manually ######### 








