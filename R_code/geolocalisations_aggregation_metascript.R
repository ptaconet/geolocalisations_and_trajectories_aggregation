
####################### Aggregation of geolocalistions data according to spatiotemporal resolution from ob7 database
# Author : Chloe Dalleau, Geomatic engineer (IRD)
# Supervisors : Paul Taconet (IRD), Julien Barde (IRD)
# Date : 15/02/2018 
# 
# wps.des: id = aggregation_location, title = Aggregation of geolocalisation data according to spatiotemporal resolution from ob7 database, abstract = Calculation of facts (ie. : catch, catch at size, effort and fad) by spatiotemporal resolution from ob7 database.;
# wps.in: id = file_name, type = character, title = Name of the R script which contains all the specific parameter of the fact (connection query and column names for sql data AND aggregation parameters : list of output dimensions - aggregate variable name - column name of object identifier - fact name) , value = "catch_balbaya|effort_balbaya|catch_at_size_t3p|catch_observe|effort_observe|catch_at_size_observe|fad_fads";
# wps.in: id = file_path_parameter, type = character, title = File path of the R script which contains all the specific parameter of the fact;
# wps.in: id = sql_limit, type = integer, title = SQL limit for the query., value = "1000";
# wps.in: id = latmin, type = integer, title = Smallest latitude of spatial zone extent in degree. Range of values: -90° to 90°., value = "-90";
# wps.in: id = latmax, type = integer, title = Biggest latitude of spatial zone extent in degree. Range of values: -90° to 90°., value = "90";
# wps.in: id = lonmin, type = integer, title =  Smallest longitude of spatial zone extent in degree. Range of values : -180° to 180°., value = "-180";
# wps.in: id = lonmax, type = integer, title =  Biggest longitude of spatial zone extent in degree. Range of values : -180° to 180°., value = "180";
# wps.in: id = spatial_grid, type = boolean, title = Put TRUE to use a spatial grid as spatial zone., value = "TRUE";
# wps.in: id = data_crs, type = boolean, title = a character string of projection arguments of data. The arguments must be entered exactly as in the PROJ.4 documentation, vale="+init=epsg:4326 +proj=longlat +datum=WGS84";
# wps.in: id = spatial_reso, type = real, title = If spatial_grid=T, Spatial resolution that fits sides of the grid square polygons in degree. Range of values: 0.001° to 5°., value = "1";
# wps.in: id = spatial_zone, type = real, title = If spatial_grid=F, SaptialDataFrame (package "sp") containing a irregular spatial zones with the same CRS of data (like : EEZ), value = NULL;
# wps.in: id = label_id_geom, type = character, title = If spatial_grid=F, label used for spatial geometry in your shapefile;
# wps.in: id = label_spatial_zone, type =character, title = Label of your spatial zone (like: grid, EEZ, ...);
# wps.in: id = temporal_reso , type = integer, title = Temporal resolution of calendar in day or month., value = "15";
# wps.in: id = temporal_reso_unit , type = character, title = Time unit of temporal resolution, value = "day|month|year";
# wps.in: id = first_date , type = date, title = First date of calendar, value = "1800-01-01";
# wps.in: id = final_date , type = date, title = Final date of calendar, value = "2100-01-01";
# wps.in: id = method_asso, type character. title = Method used for data aggregation random method (if a fishing data is on several polygons (borders case) the polygon is chosen randomly), equal distribution method (if a fishing data is on several polygons (borders case) the fishing value are distribuated between these polygons) or cwp method (The processing attributes each geolocation to an unique polygon according to CWP rules (from FAO) (http://www.fao.org/fishery/cwp/en)) are available. Value : "random|equaldistribution|cwp"
# wps.in: id = aggregate_data, type = boolean. title = Put TRUE if for aggregated data in the output, value : "TRUE|FALSE"
# wps.in: id = program_observe, type = boolean. title = For data from observe database, put TRUE to have the dimension "program" in output data, value : "TRUE|FALSE"
# wps.in: id = file_path_metadata_model, type = character. title = File path of the metadata model;
# wps.out: id = output_data, type = text/zip, title = Aggregated data by space and by time; 
#########################

# file_name
# db_user
# db_password
# sql_limit
# latmin
# latmax
# lonmin
# lonmax
# spatial_grid
# spatial_reso
# spatial_zone
# label_id_geom
# label_spatial_zone
# temporal_reso
# temporal_reso_unit
# first_date
# final_date
# method_asso
# aggregate_data
# program_observe


######################### ######################### ######################### 
# Packages
######################### ######################### ######################### 

# Packages
all_packages <- c("RPostgreSQL","tictoc","data.table","dplyr","sp","rgeos","rgdal","lubridate","stringr","rlang")
for(package in all_packages){
  if (!require(package,character.only = TRUE)) {
    install.packages(package)  
  }
  require(package,character.only = TRUE)
}
# function
source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/geolocalisations_aggregation.R")
source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/metadata_generate.R")
#time start
tic.clear()
tic()

## Set working directory of current file
######################### ######################### ######################### 
# Initialisation
######################### ######################### ######################### 
latmin <- as.numeric(latmin)
latmax <- as.numeric(latmax)
lonmin <- as.numeric(lonmin)
lonmax <- as.numeric(lonmax)
data_crs <- "+init=epsg:4326 +proj=longlat +datum=WGS84"

file_path_parameter <- paste0("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/input/geolocalisations_aggregation/",file_name,".R")
source(file_path_parameter)

#if (spatial_zone==FALSE) {
  ### EEZ exemple
  ## latitude and longitude 
   #xmin_plot=latmin
   #xmax_plot=latmax
   #ymin_plot=lonmin
   #ymax_plot=lonmax
  ## Get the EEZ from the marineregions webiste
#dsn<-paste("WFS:http://geo.vliz.be/geoserver/MarineRegions/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=MarineRegions:eez&maxFeatures=200&BBOX=",xmin_plot,",",ymin_plot,",",xmax_plot,",",ymax_plot,sep="")
#shapefile_name <- "MarineRegions:eez"
#shapefile<-readOGR(dsn,shapefile_name)
  ## Change the crs of spatial zone for the data crs 
#spatial_zone=spTransform(shapefile, CRS(data_crs))
  ## indicate the label of the spatial zone (see with str(spatial_zone))
#label_id_geom <- "geoname"
  # name of the layer (useful for metadata)
#label_spatial_zone <- "EEZ"
  ## Plot 
  # col_eez<-rgb(red=0, green=0, blue=102,alpha=70,maxColorValue=255)
  # map("world", fill=TRUE, col="gray81", bg="white", xlim=c(xmin_plot,xmax_plot),ylim=c(ymin_plot,ymax_plot))
  # plot(shape,col=col_eez,add=TRUE,lwd=0.6,border="bisque4")
#spatial_reso <-NULL
#} 



cat("Initialisation ... ok \n")


######################### ######################### ######################### 
# Database connection
######################### ######################### ######################### 

cat("Database connection in progress ... ")
## loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

## creates a connection to the postgres database
## note that "con" will be used later in each connection to the database
# call upon database manager for password (ob7@listes.ird.fr)
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
warning("Please inform the database manager of your database usage. \n This data are confidential. For diffusion please check with the database manager.")
parameter_bdd <- bdd_parameters(first_date,final_date,sql_limit)

# Add the user and password for database access
parameter_bdd$user <- db_user
parameter_bdd$password <- db_password

con <- dbConnect(drv, dbname = parameter_bdd$dbname,
                 host = parameter_bdd$host, port = parameter_bdd$port,
                 user = parameter_bdd$user, password = parameter_bdd$password)

### Logging of dataframe
# dataset contains non aggregated data
dataset<-dbGetQuery(con, parameter_bdd$query)
cat(" ok \n")

### Disconnection of data base
dbDisconnect(con)
### dataset colnames store in file_name.R
colnames(dataset) <- parameter_bdd$colnames_dataset

### Csv files case
# file_path_input_data <- paste0("fine_data/",file_name, ".csv")
# dataset <- read.csv(file_path_input_data, sep=",", header = T)

######################### ######################### ######################### 
# Treatments
######################### ######################### ######################### 
## for test
# dataset=dataset[1:750,]

### aggregation parameter
if("program" %in% colnames(dataset)){
  agg_parameters <- aggregation_parameters(program_observe)
} else {
  agg_parameters <- aggregation_parameters()
}


## nom de colonne obligatoire : time, lat, lon
output <- geolocalisations_aggregation(raw_dataset=dataset,spatial_reso=spatial_reso,latmin=latmin,
                                       latmax=latmax,lonmin=lonmin,lonmax=lonmax,data_crs =data_crs,
                           firstdate=first_date,finaldate=final_date,temporal_reso=temporal_reso,
                           temporal_reso_unit=temporal_reso_unit,aggregate_data=aggregate_data,
                           method_asso=method_asso, aggregation_parameters=agg_parameters,
                           spatial_zone=spatial_zone,label_id_geom=label_id_geom)

output_dataset <- output$data



# ######################### ######################### ######################### 
# # Metadata
# ######################### ######################### ######################### 
### Intialisation for metadata creation
# metadata model
##file_path_metadata_model <- "https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/input/geolocalisations_aggregation/metadata_input.csv"

##metadata_input <- read.csv(file_path_metadata_model, sep=",", header = T)
# metadata identifier to select the right metadata model
##metadata_id <- paste(file_name,label_spatial_zone,sep="_")
# metadata created throughout treatment
##add_metadata <- output$metadata_list
# add SQL query
##add_metadata$table_sql_query <- parameter_bdd$query
# create identifier file name
##min_date <- as_date(min(output_dataset$time_start))
##max_date <- as_date(max(output_dataset$time_end))
##start_date <- str_replace_all(min_date,"-","_")
##final_date <- str_replace_all(max_date,"-","_")
##spatial_resolution_id <- str_replace(as.character(add_metadata$spatial_resolution),fixed("."),"_")
##temporal_resolution_id <- str_replace(add_metadata$temporal_resolution,fixed("."),"_")
### data identifier
##identifier <- paste0("indian_atlantic_oceans_",agg_parameters$fact_name,"_",if(is.null(spatial_zone)){paste0(spatial_resolution_id,"deg")}else{label_spatial_zone},"_",temporal_resolution_id,
##                     add_metadata$temporal_resolution_unit,"_",start_date,"_",final_date,"_",method_asso,"_",agg_parameters$bdd_name, sep="")



##output_metadata <- metadata_generate(metadata_model=metadata_input,metadata_id=metadata_id,dataset_id=identifier,add_metadata=add_metadata)

# ######################### ######################### ######################### 
# # Create CSV file
# ######################### ######################### ######################### 

#if(dir.exists("output/geolocalisations_aggregation")==F){
#  dir.create("output")
#  dir.create("output/geolocalisations_aggregation")
#}
##filepath_dataset = paste("output/geolocalisations_aggregation/",identifier,".csv", sep="")
filepath_dataset = paste("/home/ptaconet/Bureau/docs Chloé/",file_name,".csv", sep="")
#filepath_metadata = paste("output/geolocalisations_aggregation/metadata_",identifier,".csv", sep="")

write.csv(output_dataset, file = filepath_dataset, row.names = FALSE)
#write.csv(output_metadata, file = filepath_metadata, row.names = FALSE)
