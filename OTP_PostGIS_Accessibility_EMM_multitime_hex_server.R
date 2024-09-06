library(tidyverse)
library(RPostgreSQL)
library(postGIStools)
library(geojsonR)
library(httr)
library(geojsonio)
library(stringr)
library(rpostgis)
library(svMisc)
library(scales)
library(DepartureTime)

# timer
ptm <- proc.time()

############## Update date with search and replace ###############

#currently: grid_with_residents_20201208_30min

############## testmode  ############## 
testmode = F
n_points = 50

tweet    = T
############################ 


############## modes to calculate ############## 
car =       F
pt =        T
bike =      F
ptbike =    F
bikeride =  T
############################ 

############## Spatial Scope ############## 

# search and replace:
# EMM: h e x grid_1500
# MUC: g r i d_muc_hex400


############################ 

############################ 
# Set departure times - Bike and Ride with 6 min resolution takes ~ 8 hours

deptimes <- DepartureTime(
  method = "S",
  dy = 2020,
  dm = 7,
  dd = 15,
  tmin = 08,
  tmax = 09,
  res = 6,
  MMDD = TRUE,
  ptw = FALSE
)

deptimes <- deptimes %>% 
  mutate(minutes = str_sub(Date, -5))

deptimes <- as.list(deptimes$minutes)


############################ 



############## banned routes ############## 

# banned = "1__3906%2C1__1975%2C1__3573%2C1__1618"
# banned = "1__1983304_3"
banned = ""
############### Twitterbot ##############

library(twitteR)
if (tweet == T) {
  try(setup_twitter_oauth(consumer_key = "qMmV7v3GTFI5stlFIxVFTRQDK",
                          access_token = "1085578422661926917-9kPmt4084L0twNHPcVupwlZ7ROaugf",
                          consumer_secret = "pFKS6arbpKbXNirQ1FqnNYUhpxj1sd8zbbjtxZwaHxO4yPMMxn",
                          access_secret = "4EWXvvPveu0qlX5xCM5yWTbPORZ0LxEO1BrMIBCtlSpwJ"))
}

if (tweet == T) { try(updateStatus(paste0("############ STARTING at",  format(Sys.time(), "%X"), " ############"))) }
#########################################


con <- dbConnect(PostgreSQL(),
                 dbname = "emma_db", user = "postgres",
                 #host = "195.128.100.116",
                 host = "localhost",
                 password = "internet1893"
)


for (deptime in deptimes) {







############################# CAR ############################# 
if (car == TRUE) {
  
  
  if (tweet == T) { try(updateStatus(paste0("car: starting script at ",  format(Sys.time(), "%X"))))}
  
  get_isochrone_car <- function(lat, lng, id) {
    current <- GET(
      "http://localhost:8801/otp/routers/default/isochrone",
      query = list(
        fromPlace = paste(lat, lng, sep = ","), # latlong of place
        toPlace = paste(lat, lng, sep = ","), # latlong of place - this is actually needed to make it towards the facility
        mode = "CAR", # modes we want the route planner to use
        date = "07-15-2020",
        time = deptime,
        maxWalkDistance = 800, # in metres
        walkReluctance = 1,
        minTransferTime = 0, # in secs
        arriveBy = TRUE, # time specified above is arrival time at the location
        cutoffSec = 30*60
        
      )
    )
    
    current <- content(current, as = "text", encoding = "UTF-8")
    return(current)
    
  }
  
  
  # Get cetroid points from hexgrid - import id, point, and grid cell
  points_car <- get_postgis_query(con, "SELECT id as id, st_asgeojson(st_transform(st_centroid(geom), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell FROM hexgrid_1500") #
  
  # add empty column for isochrone GeoJSON
  points_car %>% mutate("isochrone" = NA) -> points_car
  
  if (testmode == T) {
    #points_car %>% sample_n(n_points) -> points_car }
    points_car <- slice_head(points_car, n = 50) }
  
  
  for (row in 1:nrow(points_car)) {
    lon <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[1]
    lat <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[2]
    id <- points_car[row, ]$id
    
    
    iso <- get_isochrone_car(lat, lon, id) 
    #cat("\n\n\n")
    svMisc::progress((row/nrow(points_car))*100) #progress percent
    
    #loop to handle errors (e.g. no starting point found)
    if (startsWith(iso, "org.") == TRUE) {
      iso <- NA
    } else {
      if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
        iso <- NA
      } else {
        iso <- geojson_atomize(iso)
        # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
        iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
      }
    }
    
    points_car$isochrone[points_car$id == id] <- iso
    

    
  }
  
#points_car %>% head() %>%  mutate(jsoniso = geojson_sf(iso))
  
  if (tweet == T) { try(updateStatus(paste0("car: OTP loop done at ",  format(Sys.time(), "%X"))))}
  
  # save result to PostGIS DB
  dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_car;")
  try(dbWriteTable(con, name = c("public", "isochrones_car"), value = points_car)) 
  
  
  # to calculate number of *inhabitants*
  
# create filename for deptimes
  
  filename = paste0("grid_with_residents_20201207-",deptime,"_30min_car_hex") %>% str_remove(":")
  
  
  processing_query_residents <- paste0("ALTER TABLE isochrones_car ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_car RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_car SET geom = ST_Transform(st_setsrid(ST_GeomFromGeoJSON(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_car ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_car'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_car using gist(geom); -- create spatial index
ALTER TABLE isochrones_car ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 



-- sum up all points per grid cell and save result in 'residents' colum of isochrones_car 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_car g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_car g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_car set residents = 0
where residents is null;


DROP TABLE IF EXISTS ", filename, "; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
SELECT g.id, g.geom, p.residents AS residents_in_30min
INTO ", filename, "
    FROM isochrones_car AS p, hexgrid_1500 AS g
    WHERE g.id = p.id;")

try(dbSendQuery(con, processing_query_residents))


######## loop to fix wrong-empty cells ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell
points_car <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(0,100), random_between(0,100)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM grid_with_residents_20201208_30min_car_hex 
                               WHERE residents_in_30min = 0") #%>% top_n(100)

# tweet about 1st loop

if (tweet == T) { try(updateStatus(paste0("car: in first loop, ", length(points_car), "detected")))}

# add empty column for isochrone GeoJSON
points_car %>% mutate("isochrone" = NA) -> points_car

# loop
for (row in 1:nrow(points_car)) {
  lon <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[2]
  id <- points_car[row, ]$id
  
  
  iso <- get_isochrone_car(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_car))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_car$isochrone[points_car$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_car;")
try(dbWriteTable(con, name = c("public", "isochrones_car"), value = points_car)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- "ALTER TABLE isochrones_car ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_car RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_car SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_car ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_car'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_car using gist(geom); -- create spatial index
ALTER TABLE isochrones_car ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_car 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_car g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_car g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_car set residents = 0
where residents is null;


-- DROP TABLE IF EXISTS grid_with_residents_20201208_30min_car_hex; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
-- SELECT g.id, g.geom, p.residents AS residents_in_30min
-- INTO grid_with_residents_20201208_30min_car_hex
--    FROM isochrones_car AS p, hexgrid_1500 AS g
--    WHERE g.id = p.id;

UPDATE grid_with_residents_20201208_30min_car_hex
SET residents_in_30min = isochrones_car.residents
FROM isochrones_car
WHERE grid_with_residents_20201208_30min_car_hex.id = isochrones_car.id;"

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 



######## 2nd loop to fix wrong-empty cells ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell
points_car <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-100,0), random_between(-100,0)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM grid_with_residents_20201208_30min_car_hex 
                               WHERE residents_in_30min = 0") #%>% top_n(100)

# tweet about 2nd loop

if (tweet == T) { try(updateStatus(paste0("car: in second loop, ", length(points_car), "detected")))}


# add empty column for isochrone GeoJSON
points_car %>% mutate("isochrone" = NA) -> points_car

# loop
for (row in 1:nrow(points_car)) {
  lon <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_car[row, ]$centroid)$coordinates[2]
  id <- points_car[row, ]$id
  
  
  iso <- get_isochrone_car(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_car))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_car$isochrone[points_car$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_car;")
try(dbWriteTable(con, name = c("public", "isochrones_car"), value = points_car)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- "ALTER TABLE isochrones_car ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_car RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_car SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_car ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_car'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_car using gist(geom); -- create spatial index
ALTER TABLE isochrones_car ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_car 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_car g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_car g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_car set residents = 0
where residents is null;


-- DROP TABLE IF EXISTS grid_with_residents_20201208_30min_car_hex; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
-- SELECT g.id, g.geom, p.residents AS residents_in_30min
-- INTO grid_with_residents_20201208_30min_car_hex
--    FROM isochrones_car AS p, hexgrid_1500 AS g
--    WHERE g.id = p.id;

UPDATE grid_with_residents_20201208_30min_car_hex
SET residents_in_30min = isochrones_car.residents
FROM isochrones_car
WHERE grid_with_residents_20201208_30min_car_hex.id = isochrones_car.id;"

try(dbSendQuery(con, processing_query_residents))






# Tweet about final number of empty cells

# Get *empty* and  centroid points from grid - import id, point, and grid cell
points_car <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(0,100), random_between(0,100)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM grid_with_residents_20201208_30min_car_hex 
                               WHERE residents_in_30min = 0") #%>% top_n(100)

# tweet about end

if (tweet == T) { try(updateStatus(paste0("car: after second loop, ", length(points_car), "detected")))}








if (tweet == T) { try(updateStatus(paste0("car: PostGIS operations done at ",  format(Sys.time(), "%X"))))}
}




############################# PT ############################# 
if (pt == TRUE) {
  if (tweet == T) { try(updateStatus(paste0("PT: starting script at ",  format(Sys.time(), "%X"), " - for: ", deptime)))}
  
  
  get_isochrone_pt <- function(lat, lng, id) {
    current <- GET(
      "http://localhost:8801/otp/routers/default/isochrone",
      query = list(
        fromPlace = paste(lat, lng, sep = ","), # latlong of place
        toPlace = paste(lat, lng, sep = ","), # latlong of place - this is actually needed to make it towards the facility
        #mode = "TRANSIT, BICYCLE", # modes we want the route planner to use
        mode = "WALK,TRANSIT", # modes we want the route planner to use
        #mode = "CAR", # modes we want the route planner to use
        date = "07-15-2020",
        time = deptime,
        maxWalkDistance = 5000, # in metres
        walkReluctance = 5,
        minTransferTime = 0, # in secs
        arriveBy = TRUE, # time specified above is arrival time at the location
        cutoffSec = 30*60,
        bannedRoutes = banned
        
        
      )
    )
    
    current <- content(current, as = "text", encoding = "UTF-8")
    return(current)
    
  }
  
  
  # Get centroid points from grid - import id, point, and grid cell
  points_pt <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(st_centroid(geom), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell FROM hexgrid_1500") #%>% top_n(100)
  
  
  # add empty column for isochrone GeoJSON
  points_pt %>% mutate("isochrone" = NA) -> points_pt
  
  if (testmode == T) {
    points_pt %>% sample_n(n_points) -> points_pt }
  
  #points_pt %>% filter(id == "1kmN2776E4422") -> points_pt
  
  for (row in 1:nrow(points_pt)) {
    lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
    lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
    id <- points_pt[row, ]$id
    
    
    iso <- get_isochrone_pt(lat, lon, id)
    #cat("\n\n\n")
    svMisc::progress((row/nrow(points_pt))*100) #progress percent
    
    #loop to handle errors (e.g. no starting point found)
    if (startsWith(iso, "org.") == TRUE) {
      iso <- NA
    } else {
      if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
        iso <- NA
      } else {
        iso <- geojson_atomize(iso)
        # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
        iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
      }
    }
    
    points_pt$isochrone[points_pt$id == id] <- iso
    
  }
  
  if (tweet == T) { try(updateStatus(paste0("PT: OTP loop done at ",  format(Sys.time(), "%X"))))}
  
  # save result to PostGIS DB
  dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_pt;")
  #try(pgInsert(con, name = c("public", "isochrones_pt"), data.obj = points_pt)) -- old version, did not work
  try(dbWriteTable(con, name = c("public", "isochrones_pt"), value = points_pt)) # -- new!
  
  
  # to calculate number of *inhabitants*
  
  filename = paste0("grid_with_residents_20201207_",deptime,"_30min_pt_hex") %>% str_remove(":")
  
  processing_query_residents <- paste0("ALTER TABLE isochrones_pt ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_pt RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_pt SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_pt ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_pt'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_pt using gist(geom); -- create spatial index
ALTER TABLE isochrones_pt ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_pt 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_pt g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_pt g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_pt set residents = 0
where residents is null;


DROP TABLE IF EXISTS ", filename, "; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
SELECT g.id, g.geom, p.residents AS residents_in_30min
INTO ", filename, "
    FROM isochrones_pt AS p, hexgrid_1500 AS g
    WHERE g.id = p.id;")

try(dbSendQuery(con, processing_query_residents))
if (tweet == T) { try(updateStatus(paste0("PT: PostGIS operations done at ",  format(Sys.time(), "%X"), " - for: ", deptime)))}



######## loop to fix wrong-empty cells (1) ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell

query <- paste0("SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-750,750), random_between(-750,750)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM ", filename, " 
                               WHERE residents_in_30min = 0")

points_pt <- get_postgis_query(con, query) #%>% top_n(100)

# add empty column for isochrone GeoJSON
points_pt %>% mutate("isochrone" = NA) -> points_pt

# loop
for (row in 1:nrow(points_pt)) {
  lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
  id <- points_pt[row, ]$id
  
  
  iso <- get_isochrone_pt(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_pt))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_pt$isochrone[points_pt$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_pt;")
try(dbWriteTable(con, name = c("public", "isochrones_pt"), value = points_pt)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- paste0("ALTER TABLE isochrones_pt ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_pt RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_pt SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_pt ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_pt'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_pt using gist(geom); -- create spatial index
ALTER TABLE isochrones_pt ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_pt 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_pt g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_pt g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_pt set residents = 0
where residents is null;


-- DROP TABLE IF EXISTS grid_with_residents_20201208_30min_pt_hex2; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
-- SELECT g.id, g.geom, p.residents AS residents_in_30min
-- INTO grid_with_residents_20201208_30min_pt_hex2
--    FROM isochrones_pt AS p, hexgrid_1500 AS g
--    WHERE g.id = p.id;

UPDATE ", filename, "
SET residents_in_30min = isochrones_pt.residents
FROM isochrones_pt
WHERE ", filename, ".id = isochrones_pt.id;")

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 

######## loop to fix wrong-empty cells (2) ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell

query <- paste0("SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-750,750), random_between(-750,750)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM ", filename, " 
                               WHERE residents_in_30min = 0")

points_pt <- get_postgis_query(con, query) #%>% top_n(100)

# add empty column for isochrone GeoJSON
points_pt %>% mutate("isochrone" = NA) -> points_pt

# loop
for (row in 1:nrow(points_pt)) {
  lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
  id <- points_pt[row, ]$id
  
  
  iso <- get_isochrone_pt(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_pt))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_pt$isochrone[points_pt$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_pt;")
try(dbWriteTable(con, name = c("public", "isochrones_pt"), value = points_pt)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- paste0("ALTER TABLE isochrones_pt ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_pt RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_pt SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_pt ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_pt'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_pt using gist(geom); -- create spatial index
ALTER TABLE isochrones_pt ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_pt 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_pt g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_pt g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_pt set residents = 0
where residents is null;


-- DROP TABLE IF EXISTS grid_with_residents_20201208_30min_pt_hex2; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
-- SELECT g.id, g.geom, p.residents AS residents_in_30min
-- INTO grid_with_residents_20201208_30min_pt_hex2
--    FROM isochrones_pt AS p, hexgrid_1500 AS g
--    WHERE g.id = p.id;

UPDATE ", filename, "
SET residents_in_30min = isochrones_pt.residents
FROM isochrones_pt
WHERE ", filename, ".id = isochrones_pt.id;")

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 

######## loop to fix wrong-empty cells (3) ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell

query <- paste0("SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-750,750), random_between(-750,750)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM ", filename, " 
                               WHERE residents_in_30min = 0")

points_pt <- get_postgis_query(con, query) #%>% top_n(100)

# add empty column for isochrone GeoJSON
points_pt %>% mutate("isochrone" = NA) -> points_pt

# loop
for (row in 1:nrow(points_pt)) {
  lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
  id <- points_pt[row, ]$id
  
  
  iso <- get_isochrone_pt(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_pt))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_pt$isochrone[points_pt$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_pt;")
try(dbWriteTable(con, name = c("public", "isochrones_pt"), value = points_pt)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- paste0("ALTER TABLE isochrones_pt ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_pt RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_pt SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_pt ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_pt'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_pt using gist(geom); -- create spatial index
ALTER TABLE isochrones_pt ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_pt 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_pt g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_pt g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_pt set residents = 0
where residents is null;


-- DROP TABLE IF EXISTS grid_with_residents_20201208_30min_pt_hex2; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
-- SELECT g.id, g.geom, p.residents AS residents_in_30min
-- INTO grid_with_residents_20201208_30min_pt_hex2
--    FROM isochrones_pt AS p, hexgrid_1500 AS g
--    WHERE g.id = p.id;

UPDATE ", filename, "
SET residents_in_30min = isochrones_pt.residents
FROM isochrones_pt
WHERE ", filename, ".id = isochrones_pt.id;")

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 


if (tweet == T) { try(updateStatus(paste0("PT: ALL PostGIS operations done at ",  format(Sys.time(), "%X"), " - for: ", deptime)))}


}
############################# bike #############################
if (bike == TRUE) {
  if (tweet == T) {try(updateStatus(paste0("bike: starting script at ",  format(Sys.time(), "%X"))))}
  
  get_isochrone_bike <- function(lat, lng, id) {
    current <- GET(
      "http://localhost:8801/otp/routers/default/isochrone",
      query = list(
        fromPlace = paste(lat, lng, sep = ","), # latlong of place
        toPlace = paste(lat, lng, sep = ","), # latlong of place - this is actually needed to make it towards the facility
        mode = "BICYCLE", # modes we want the route planner to use
        #mode = "WALK, TRANSIT", # modes we want the route planner to use
        #mode = "CAR", # modes we want the route planner to use
        date = "07-15-2020",
        time = deptime,
        maxWalkDistance = 15000, # in metres
        walkReluctance = 1,
        #minTransferTime = 60, # in secs
        arriveBy = TRUE, # time specified above is arrival time at the location
        # cutoffSec = 900,
        cutoffSec = 30*60
        # cutoffSec = 2700,
        # cutoffSec = 3600
      )
    )
    
    current <- content(current, as = "text", encoding = "UTF-8")
    return(current)
    
  }
  
  
  # Get cetroid points from grid - import id, point, and grid cell
  points_bike <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(st_centroid(geom), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell FROM hexgrid_1500") #%>% top_n(100)
  
  
  # add empty column for isochrone GeoJSON
  points_bike %>% mutate("isochrone" = NA) -> points_bike
  
  if (testmode == T) {
    points_bike %>% sample_n(n_points) -> points_bike }
  
  #points_bike %>% filter(id == "1kmN2776E4422") -> points_bike
  
  for (row in 1:nrow(points_bike)) {
    lon <- FROM_GeoJson(points_bike[row, ]$centroid)$coordinates[1]
    lat <- FROM_GeoJson(points_bike[row, ]$centroid)$coordinates[2]
    id <- points_bike[row, ]$id
    
    
    iso <- get_isochrone_bike(lat, lon, id)
    #cat("\n\n\n")
    svMisc::progress((row/nrow(points_bike))*100) #progress percent
    
    #loop to handle errors (e.g. no starting point found)
    if (startsWith(iso, "org.") == TRUE) {
      iso <- NA
    } else {
      if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
        iso <- NA
      } else {
        iso <- geojson_atomize(iso)
        # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
        iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
      }
    }
    
    points_bike$isochrone[points_bike$id == id] <- iso
    
  }
  
  if (tweet == T) {try(updateStatus(paste0("bike: OTP loop done at ",  format(Sys.time(), "%X"))))}
  
  # save result to PostGIS DB
  dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_bike;")
  #try(pgInsert(con, name = c("public", "isochrones_bike"), data.obj = points_bike)) -- old version, did not work
  try(dbWriteTable(con, name = c("public", "isochrones_bike"), value = points_bike)) # -- new!
  
  
  # to calculate number of *inhabitants*
  processing_query_residents <- "ALTER TABLE isochrones_bike ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_bike RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_bike SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_bike ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_bike'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_bike using gist(geom); -- create spatial index
ALTER TABLE isochrones_bike ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_bike
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_bike g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_bike g set residents = x.sum_residents
from x
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_bike set residents = 0
where residents is null;


DROP TABLE IF EXISTS grid_with_residents_20201208_30min_bike_hex; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
SELECT g.id, g.geom, p.residents AS residents_in_30min
INTO grid_with_residents_20201208_30min_bike_hex
    FROM isochrones_bike AS p, hexgrid_1500 AS g
    WHERE g.id = p.id;"

dbSendQuery(con, processing_query_residents)

if (tweet == T) {try(updateStatus(paste0("bike: PostGIS operations done at ",  format(Sys.time(), "%X"))))}
}
############################# PT + Bike #############################
if (ptbike == TRUE) {
  if (tweet == T) {try(updateStatus(paste0("PT+bike: starting script at ",  format(Sys.time(), "%X"))))}
  
  get_isochrone_ptbike <- function(lat, lng, id) {
    current <- GET(
      "http://localhost:8801/otp/routers/default/isochrone",
      query = list(
        fromPlace = paste(lat, lng, sep = ","), # latlong of place
        toPlace = paste(lat, lng, sep = ","), # latlong of place - this is actually needed to make it towards the facility
        mode = "TRANSIT,BICYCLE", # modes we want the route planner to use
        #mode = "WALK, TRANSIT", # modes we want the route planner to use
        #mode = "CAR", # modes we want the route planner to use
        date = "07-15-2020",
        time = deptime,
        maxWalkDistance = 5000, # in metres
        walkReluctance = 1,
        minTransferTime = 0, # in secs
        arriveBy = TRUE, # time specified above is arrival time at the location
        # cutoffSec = 900,
        cutoffSec = 30*60
        # cutoffSec = 2700,
        # cutoffSec = 3600
      )
    )
    
    current <- content(current, as = "text", encoding = "UTF-8")
    return(current)
    
  }
  
  if (tweet == T) { try(updateStatus(paste0("PT+bike: OTP loop done at ",  format(Sys.time(), "%X"))))}
  
  # Get cetroid points from grid - import id, point, and grid cell
  points_ptbike <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(st_centroid(geom), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell FROM hexgrid_1500") # %>% top_n(100)
  
  
  # add empty column for isochrone GeoJSON
  points_ptbike %>% mutate("isochrone" = NA) -> points_ptbike
  
  if (testmode == T) {
    points_ptbike %>% sample_n(n_points) -> points_ptbike }
  
  #points_ptbike %>% filter(id == "1kmN2776E4422") -> points_ptbike
  
  for (row in 1:nrow(points_ptbike)) {
    lon <- FROM_GeoJson(points_ptbike[row, ]$centroid)$coordinates[1]
    lat <- FROM_GeoJson(points_ptbike[row, ]$centroid)$coordinates[2]
    id <- points_ptbike[row, ]$id
    
    
    iso <- get_isochrone_ptbike(lat, lon, id)
    #cat("\n\n\n")
    svMisc::progress((row/nrow(points_ptbike))*100) #progress percent
    
    #loop to handle errors (e.g. no starting point found)
    if (startsWith(iso, "org.") == TRUE) {
      iso <- NA
    } else {
      if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
        iso <- NA
      } else {
        iso <- geojson_atomize(iso)
        # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
        iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
      }
    }
    
    points_ptbike$isochrone[points_ptbike$id == id] <- iso
    
  }
  
  
  # save result to PostGIS DB
  dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_ptbike;")
  #try(pgInsert(con, name = c("public", "isochrones_ptbike"), data.obj = points_ptbike)) -- old version, did not work
  try(dbWriteTable(con, name = c("public", "isochrones_ptbike"), value = points_ptbike)) # -- new!
  
  
  # to calculate number of *inhabitants*
  processing_query_residents <- "ALTER TABLE isochrones_ptbike ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_ptbike RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_ptbike SET geom = ST_Transform(st_setsrid(ST_GeomFromGeoJSON(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_ptbike ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_ptbike'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_ptbike using gist(geom); -- create spatial index
ALTER TABLE isochrones_ptbike ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_ptbike
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_ptbike g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_ptbike g set residents = x.sum_residents
from x
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_ptbike set residents = 0
where residents is null;


DROP TABLE IF EXISTS grid_with_residents_20201208_30min_ptbike_hex; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
SELECT g.id, g.geom, p.residents AS residents_in_30min
INTO grid_with_residents_20201208_30min_ptbike_hex
    FROM isochrones_ptbike AS p, hexgrid_1500 AS g
    WHERE g.id = p.id;"

dbSendQuery(con, processing_query_residents)

if (tweet == T) {try(updateStatus(paste0("PT+bike: PostGIS operations done at ",  format(Sys.time(), "%X"))))}
}

############################# Bike & Ride #############################
if (bikeride == TRUE) {
  if (tweet == T) {try(updateStatus(paste0("Bike+Ride: starting script at ",  format(Sys.time(), " - for: ", deptime))))}
  
  get_isochrone_bikeride <- function(lat, lng, id) {
    current <- GET(
      "http://localhost:8801/otp/routers/default/isochrone",
      query = list(
        fromPlace = paste(lat, lng, sep = ","), # latlong of place
        toPlace = paste(lat, lng, sep = ","), # latlong of place - this is actually needed to make it towards the facility
        mode = "BICYCLE_PARK,WALK,TRANSIT", # modes we want the route planner to use
        #mode = "WALK, TRANSIT", # modes we want the route planner to use
        #mode = "CAR", # modes we want the route planner to use
        date = "07-15-2020",
        time = deptime,
        maxWalkDistance = 5000, # in metres
        walkReluctance = 1,
        minTransferTime = 0, # in secs
        arriveBy = TRUE, # time specified above is arrival time at the location
        # cutoffSec = 900,
        cutoffSec = 30*60
        # cutoffSec = 2700,
        # cutoffSec = 3600
      )
    )
    
    current <- content(current, as = "text", encoding = "UTF-8")
    return(current)
    
  }
  
  if (tweet == T) { try(updateStatus(paste0("Bike+Ride: OTP loop done at ",  format(Sys.time(), "%X"))))}
  
  # Get cetroid points from grid - import id, point, and grid cell
  points_bikeride <- get_postgis_query(con, "SELECT id, st_asgeojson(st_transform(st_centroid(geom), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell FROM hexgrid_1500") # %>% top_n(100)
  
  
  # add empty column for isochrone GeoJSON
  points_bikeride %>% mutate("isochrone" = NA) -> points_bikeride
  
  if (testmode == T) {
    points_bikeride %>% sample_n(n_points) -> points_bikeride }
  
  #points_bikeride %>% filter(id == "1kmN2776E4422") -> points_bikeride
  
  for (row in 1:nrow(points_bikeride)) {
    lon <- FROM_GeoJson(points_bikeride[row, ]$centroid)$coordinates[1]
    lat <- FROM_GeoJson(points_bikeride[row, ]$centroid)$coordinates[2]
    id <- points_bikeride[row, ]$id
    
    
    iso <- get_isochrone_bikeride(lat, lon, id)
    #cat("\n\n\n")
    svMisc::progress((row/nrow(points_bikeride))*100) #progress percent
    
    #loop to handle errors (e.g. no starting point found)
    if (startsWith(iso, "org.") == TRUE) {
      iso <- NA
    } else {
      if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
        iso <- NA
      } else {
        iso <- geojson_atomize(iso)
        # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
        iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
      }
    }
    
    points_bikeride$isochrone[points_bikeride$id == id] <- iso
    
  }
  
  
  # save result to PostGIS DB
  dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_bikeride;")
  #try(pgInsert(con, name = c("public", "isochrones_bikeride"), data.obj = points_bikeride)) -- old version, did not work
  try(dbWriteTable(con, name = c("public", "isochrones_bikeride"), value = points_bikeride)) # -- new!

  
    
  
  # to calculate number of *inhabitants*
  
  filename = paste0("grid_with_residents_20201207_",deptime,"_30min_bikeride_hex") %>% str_remove(":")
  
  
  processing_query_residents <- paste0("ALTER TABLE isochrones_bikeride ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_bikeride RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_bikeride SET geom = ST_Transform(st_setsrid(ST_GeomFromGeoJSON(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_bikeride ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_bikeride'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_bikeride using gist(geom); -- create spatial index
ALTER TABLE isochrones_bikeride ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_bikeride
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_bikeride g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_bikeride g set residents = x.sum_residents
from x
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_bikeride set residents = 0
where residents is null;


DROP TABLE IF EXISTS ", filename, "; -- delete old table
-- JOIN: add residents within 30-min-isochrone to hexgrid_1500 in column residents_in_30min (by ID)
SELECT g.id, g.geom, p.residents AS residents_in_30min
INTO ", filename, "
    FROM isochrones_bikeride AS p, hexgrid_1500 AS g
    WHERE g.id = p.id;")

dbSendQuery(con, processing_query_residents)
if (tweet == T) { try(updateStatus(paste0("Bike+Ride: PostGIS operations done at ",  format(Sys.time(), "%X"), " - for: ", deptime)))}

######## loop to fix wrong-empty cells (1) ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell

query <- paste0("SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-750,750), random_between(-750,750)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM ", filename, " 
                               WHERE residents_in_30min = 0")

points_pt <- get_postgis_query(con, query) #%>% top_n(100)

# add empty column for isochrone GeoJSON
points_pt %>% mutate("isochrone" = NA) -> points_pt

# loop
for (row in 1:nrow(points_pt)) {
  lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
  id <- points_pt[row, ]$id
  
  
  iso <- get_isochrone_pt(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_pt))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_pt$isochrone[points_pt$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_bikeride;")
try(dbWriteTable(con, name = c("public", "isochrones_bikeride"), value = points_bikeride)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- paste0("ALTER TABLE isochrones_bikeride ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_bikeride RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_bikeride SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_bikeride ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_bikeride'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_bikeride using gist(geom); -- create spatial index
ALTER TABLE isochrones_bikeride ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_bikeride 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_bikeride g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_bikeride g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_bikeride set residents = 0
where residents is null;


UPDATE ", filename, "
SET residents_in_30min = isochrones_bikeride.residents
FROM isochrones_bikeride
WHERE ", filename, ".id = isochrones_bikeride.id;")

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 

######## loop to fix wrong-empty cells (2) ######## 


# Get *empty* and *shifted* centroid points from grid - import id, point, and grid cell

query <- paste0("SELECT id, st_asgeojson(st_transform(
st_translate(
st_centroid(geom), random_between(-750,750), random_between(-750,750)), 4326))
                            AS centroid, st_asgeojson(st_transform(geom, 4326)) AS cell, 
                            residents_in_30min FROM ", filename, " 
                               WHERE residents_in_30min = 0")

points_pt <- get_postgis_query(con, query) #%>% top_n(100)

# add empty column for isochrone GeoJSON
points_pt %>% mutate("isochrone" = NA) -> points_pt

# loop
for (row in 1:nrow(points_pt)) {
  lon <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[1]
  lat <- FROM_GeoJson(points_pt[row, ]$centroid)$coordinates[2]
  id <- points_pt[row, ]$id
  
  
  iso <- get_isochrone_pt(lat, lon, id)
  #cat("\n\n\n")
  svMisc::progress((row/nrow(points_pt))*100) #progress percent
  
  #loop to handle errors (e.g. no starting point found)
  if (startsWith(iso, "org.") == TRUE) {
    iso <- NA
  } else {
    if (startsWith(iso, "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":null")) {
      iso <- NA
    } else {
      iso <- geojson_atomize(iso)
      # make the resulting JSON a MultiPolygon instead of FeatureCollection by removing some strings
      iso <- str_remove(iso, stringr::fixed("{\"type\":\"Feature\",\"geometry\":"))
    }
  }
  
  points_pt$isochrone[points_pt$id == id] <- iso
  
}

# save result to PostGIS DB
dbSendQuery(con, "DROP TABLE IF EXISTS isochrones_bikeride;")
try(dbWriteTable(con, name = c("public", "isochrones_bikeride"), value = points_bikeride)) # -- new!

# to calculate number of *inhabitants*
processing_query_residents <- paste0("ALTER TABLE isochrones_bikeride ADD PRIMARY KEY (id); -- add PK
ALTER TABLE isochrones_bikeride RENAME COLUMN isochrone TO geom; -- rename col
UPDATE  isochrones_bikeride SET geom = ST_Transform(st_setsrid(ST_Geomfromgeojson(geom), 4326), 3857); -- transform geom column first to correct SRID, then reproject to 3857
ALTER TABLE isochrones_bikeride ALTER COLUMN geom TYPE Geometry USING geom::Geometry; -- change geom column from text to geometry
SELECT Populate_Geometry_Columns('public.isochrones_bikeride'::regclass); -- register geometry (now it is a multipolygon)
create index on isochrones_bikeride using gist(geom); -- create spatial index
ALTER TABLE isochrones_bikeride ADD COLUMN residents INTEGER; --add empty residents column
-- simplify geometry
UPDATE  isochrones_car SET geom = ST_Simplify(geom, 100); 


-- sum up all points per grid cell and save result in 'residents' colum of isochrones_bikeride 
with x as (
    select g.id, sum(p.einwohner) as sum_residents
    from zensus100m_emm_ew p, isochrones_bikeride g
    where ST_Intersects(p.geom, g.geom)
    group by g.id
)
update isochrones_bikeride g set residents = x.sum_residents
from x 
where x.id = g.id;

-- make all cells with 0 residents a real '0' instead of null
update isochrones_bikeride set residents = 0
where residents is null;


UPDATE ", filename, "
SET residents_in_30min = isochrones_bikeride.residents
FROM isochrones_bikeride
WHERE ", filename, ".id = isochrones_bikeride.id;")

try(dbSendQuery(con, processing_query_residents))



######## / loop to fix wrong-empty cells ######## 

if (tweet == T) { try(updateStatus(paste0("Bike+Ride: ALL PostGIS operations done at ",  format(Sys.time(), "%X"), " - for: ", deptime)))}


}

  
}  
  
dbDisconnect(con)


if (tweet == T) { 
  try(updateStatus(paste0("####### All processes terminated at ",  format(Sys.time(), "%X"), " #######"))) }

# timer
proc.time() - ptm
