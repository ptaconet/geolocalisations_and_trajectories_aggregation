
####################### Aggregation of trajectories by dimensions, by time and by space
# Author : Chloe Dalleau, Geomatic engineer (IRD)
# Supervisors : Paul Taconet (IRD), Julien Barde (IRD)
# Date : 21/02/2018 
# 
# wps.des: id = trajectories_aggregation, title = Aggregation of trajectories by dimensions, by time and by space, abstract = Calculation of total distance traveled and exploited surface by object and aggregation of data by dimenions, by time and by time.;
# wps.in: id = csv_input_name, type = character, title = Name of input data (with extension, like : "boat_2017.csv") ;
# wps.in: id = file_path_input_data, type = character, title = File path of the input data;
# wps.in: id = public_link_input_data, type = character, title = Public link to access the input data;
# wps.in: id = object_type, type = character, title = Object type (like : "vms", "ais", "fad");
# wps.in: id = colname_idobject, type = character, title = Column name in the input data for object identifier;
# wps.in: id = colname_idtraj, type = character, title = Column name in the input data for trajectories identifier;
# wps.in: id = colname_time, type = character, title = Column name in the input data for date time;
# wps.in: id = colname_lat, type = character, title = Column name in the input data for latitude;
# wps.in: id = colname_lon, type = character, title = Column name in the input data for longitude;
# wps.in: id = buffer_size, type = integer, title = size of the buffer around the trajectory to calculate the surface exploited (unit:km), type=integer;
# wps.in: id = lat_min, type = integer, title = Smallest latitude of spatial zone extent in degree. Range of values: -90° to 90°., value = "-90";
# wps.in: id = lat_max, type = integer, title = Biggest latitude of spatial zone extent in degree. Range of values: -90° to 90°., value = "90";
# wps.in: id = lon_min, type = integer, title =  Smallest longitude of spatial zone extent in degree. Range of values : -180° to 180°., value = "-180";
# wps.in: id = lon_max, type = integer, title =  Biggest longitude of spatial zone extent in degree. Range of values : -180° to 180°., value = "180";
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
# wps.in: id = aggregate_data, type = boolean. title = Put TRUE if for aggregated data in the output, value : "TRUE|FALSE"
# wps.in: id = list_dim_output, type = boolean. title = List of dmensions from input data and for output data (like : c("flag","gear"), NULL, ...), value : "TRUE|FALSE"
# wps.in: id = file_path_metadata_model, type = character. title = File path of the metadata model;
# wps.out: id = output_data, type = text/zip, title = Aggregated data by dimensions, by space and by time; 
#########################


######################## Packages
all_packages <- c("rstudioapi","data.table","dplyr","lubridate","raster","rgeos","sp","rgdal","stringr","tictoc","rlang", "SDLfilter")
for(package in all_packages){
  if (!require(package,character.only = TRUE)) {
    install.packages(package)  
  }
  require(package,character.only = TRUE)
}
source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/trajectories_aggregation.R")
source("https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/metadata_generate.R")



# timer : 
tic.clear()
tic()

######################### Set working directory of current file
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

######################## Input data 

## name of the csv input, this file has to be a folder named "input" (with extension)
csv_input_name <- "fad_extract_5000.csv"
file_path_input_data <- paste0("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/input/trajectories_aggregation/",csv_input_name)
public_link_input_data <- NA
institute_source <- "IRD"
## file caracteristics (object_type in lower case)
object_type <- "fad" 
colname_idobject="fad_id"
colname_idtraj= "section_id"
colname_time= "date"
colname_lat= "lat_fad"
colname_lon = "lon_fad"

## buffer size from the object in km
buffer_size <- 10 # km
## time extent , tz = "UTC"
first_date <-"2000-01-01"
final_date <- "2017-12-31"
# for mi-month put temporal_reso = 1/2 and temporal_reso_unit "month"
## temporal resolution and unit
temporal_reso <- 1
temporal_reso_unit <- "month"
## CRS of input data
data_crs <- "+init=epsg:4326 +proj=longlat +datum=WGS84"
## aggregate data 
aggregate_data = T
if (aggregate_data == T){
  # dimension list from input data and which will appear in the outputdata
  list_dim_output <- c("fad_class")
  # list_dim_output <- NULL
  
}
### spatial resolution
spatial_grid <- T
## zone extent : latittude (in degree) range -90:90, longitude (in degree) range -180:180
lat_min <- -90
lat_max <- 90
lon_min <- -180
lon_max <- 180
if (spatial_grid==T) {
  ### Definition of spatial grid, squares compound
  ## data spatial resoltion in degree
  spatial_reso <- 1
  spatial_zone=NULL
  label_id_geom = NULL
  label_spatial_zone = "grid"
} else {
  ### EEZ exemple
  ## latitude and longitude 
  ## WARNING shapefile downloading can be long : don't select a large area
  xmin_plot=lat_min
  xmax_plot=lat_max
  ymin_plot=lon_min
  ymax_plot=lon_max
  ## Get the EEZ from the marineregions webiste
  dsn<-paste("WFS:http://geo.vliz.be/geoserver/MarineRegions/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=MarineRegions:eez&maxFeatures=200&BBOX=",xmin_plot,",",ymin_plot,",",xmax_plot,",",ymax_plot,sep="")
  eez_marineregion_layer<-readOGR(dsn,"MarineRegions:eez")
  ## Change the crs of spatial zone for the data crs 
  spatial_zone=spTransform(eez_marineregion_layer, CRS(data_crs))
  ## indicate the label of the spatial zone
  label_id_geom <- "geoname"
  label_spatial_zone <- "EEZ"
  ## Plot 
  # col_eez<-rgb(red=0, green=0, blue=102,alpha=70,maxColorValue=255)
  # map("world", fill=TRUE, col="gray81", bg="white", xlim=c(xmin_plot,xmax_plot),ylim=c(ymin_plot,ymax_plot))
  # plot(eez_marineregion_layer,col=col_eez,add=TRUE,lwd=0.6,border="bisque4")
  spatial_reso <-NULL
} 

# metadata model
file_path_metadata_model <- "https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/input/trajectories_aggregation/metadata_input.csv"


# ######################### ######################### ######################### 
# # Import data from Workspace 
# ######################### ######################### ######################### 

# ## Download from Workspace 
# userconfig_data <- read.csv("~/userconfig.csv", header = F, sep = ";", stringsAsFactors = F)
# username<<- as.character(userconfig_data[[9]])
# token<<- as.character(userconfig_data[[10]])
# ## Define the Workspace where downloading R functions
# dataWS <- paste0('/Home/',username,'/Workspace/VRE Folders/RStudioLab/rasterisation_trajectoire/Inputs/FADs')
# # # file_path_input_data <- "vms_2016.csv"
# # name <- "rawdata_8FADs_v1.csv"
# ### Download from Workspace 
# # file_path_source <- paste0(dataWS,"/",name)

# downloadFileWS(paste(dataWS,paste0("/",file_path_input_data),sep=""))
input_dataset <- read.csv(file_path_input_data, sep=",", header = T)




# ######################### ######################### ######################### 
# # Treatment
# ######################### ######################### ######################### 
### Pre-processing to select column in the input data and change the name of few column 
### for trajectories_aggregation function ("id_object", "id_traj", "time", "lat", "lon")


dataset <- preprocessing_data_trajectories(dataset=input_dataset,list_dim_output,colname_idobject,
                                           colname_idtraj, colname_time,colname_lat,colname_lon)

rm(input_dataset)

### test
# dataset <- dataset[1:4000,]

### treatment
output <- trajectories_aggregation(dataset,buffer_size,spatial_reso,lat_min,lat_max
                                   ,lon_min,lon_max,firstdate=first_date,finaldate=final_date,temporal_reso,temporal_reso_unit,
                                   aggregate_data, list_dim_output=list_dim_output,spatial_zone=spatial_zone ,
                                   label_id_geom=label_id_geom)

### Store dataset
output_dataset <- output$data


# ######################### ######################### ######################### 
# # Metadata
# ######################### ######################### ######################### 
### list of calculate variables
vars_label_list <- c("distance", "normalize_distance","surface","normalize_surface","number_of_trajectories")
### Intialisation for metadata creation
# metadata model
metadata_input <- read.csv(file_path_metadata_model, sep=",", header = T)
# metadata identifier to select the right metadata model
metadata_id <- paste0(object_type,"_",vars_label_list,if(aggregate_data==T){"_aggregation"},"_",label_spatial_zone)
# metadata created throughout treatment
add_metadata <- output$metadata_list
# create identifier file name
min_date <- as_date(min(output_dataset$time_start))
max_date <- as_date(max(output_dataset$time_end))
start_date <- str_replace_all(min_date,"-","_")
final_date <- str_replace_all(max_date,"-","_")
spatial_resolution_id <- str_replace(as.character(add_metadata$spatial_resolution),fixed("."),"_")
temporal_resolution_id <- str_replace(add_metadata$temporal_resolution,fixed("."),"_")
identifier <- paste0("atlantic_indian_oceans_",object_type,if(aggregate_data==T){"_aggregated"},"_trajectories_",start_date,"_",final_date,"_",if(is.null(spatial_zone)){paste0(spatial_resolution_id,"deg","_")}else{label_spatial_zone},temporal_resolution_id,temporal_reso_unit,"_",institute_source)
# add identifier in metadata to add
add_metadata$identifier <- identifier

### create metadata
output_metadata <- metadata_by_var(vars_label_list,metadata_model=metadata_input, metadata_id,add_metadata=add_metadata)


# ######################### ######################### ######################### 
# # Create CSV file
# ######################### ######################### ######################### 

if(dir.exists("output/trajectories_aggregation")==F){
  dir.create("output")
  dir.create("output/trajectories_aggregation")
}
filepath_dataset = paste("output/trajectories_aggregation/",identifier,".csv", sep="")
filepath_metadata = paste("output/trajectories_aggregation/metadata_",identifier,".csv", sep="")

write.csv(output_dataset, file = filepath_dataset, row.names = FALSE)
write.csv(output_metadata, file = filepath_metadata, row.names = FALSE)



toc()


