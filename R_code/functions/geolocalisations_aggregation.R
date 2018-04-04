####################### List of functions
# Author : Chloé Dalleau, Geomatic engineer (IRD)
# Supervisor : Julien Barde (IRD)
# Date : 15/02/2018 
# 
# ## Summary 
# 1. geolocation_aggregation: aggregates data by dimensions, by space and by time
# 2. number_boat: calculate number of boats for effort fact by dimensions, by space and by time
# 3. number_fad_and_days : calculate and aggregate the number of FAD and days by dimensions, by space and by time


geolocalisations_aggregation <- function(df_input,
                                         grid_spatial_resolution,
                                         intersection_layer=NULL,
                                         data_crs="+init=epsg:4326 +proj=longlat +datum=WGS84" ,
                                         temporal_resolution,
                                         temporal_resolution_unit,
                                         first_date=NULL,
                                         final_date=NULL,
                                         aggregate_data=TRUE,
                                         spatial_association_method="equaldistribution")
{
  
  #' @name geolocation_aggregation
  #' @title Geolocation aggregation by dimensions, by space and by time
  #' @description Take a data frame with geolocalisation dimension ("lon" for longitude and "lat" for latitude), date time dimension ("time"), several data dimensions (like flag, gear...), a variable to aggregate (like catch, effort, catch at size). Generate a spatial grid with input parameter (grid extent, spatial resolution) or take an input shapefile with spatial polygons from the package sp and "label_id_geom". Generate a continuous calendar of period (day, 1/2 month, month, year) with input parameter (calendar limits, temporal resolution and temporal resolution unit). The algorithm associates input data with intersect spatial zone and intersect calendar period. When geolocalisation data are on spatial zone boundaries, 3 methods are available: (1) "equaldistribution": variable is distributed between spatial zones (equal share); (2) "random": spatial zone is chosen randomly from intersected spatial zones;  (3) "cwp": Coordinating Working Party on fishery Statistics rules establish by the Food and Agriculture Organization of the United Nations FAO. (for more information: http://www.fao.org/fishery/cwp/en). If aggregate_data is TRUE, aggregate input data by data dimensions, by space and by time. if not, the algorithm associates input data with intersect spatial zone and intersect calendar period.   
  #' @param df_input dataframe with geolocalisation objects and containing: object identifier, data dimensions,"time" for geolocalisation date time , "lat" for latitude of the position in degree, "lon"  for longitude of the position in degree, type = data.frame;
  #' @param grid_spatial_resolution spatial resolution of the grid in degree, type = integer;
  #' @param data_crs a character string of projection arguments of data. The arguments must be entered exactly as in the PROJ.4 documentation, value="+init=epsg:4326 +proj=longlat +datum=WGS84", type = character;
  #' @param temporal_resolution temporal resolution of the calendar in day, month or year (see: temporal_resolution_unit). Note: for 1/2 month put temporal_resolution=1/2 and temporal_resolution_unit="month" , type = integer;
  #' @param temporal_resolution_unit temportal resolution unit od calendar, accepted value : "day" "month" "year", type = character;
  #' @param aggregate_data Put TRUE if you want aggregated data in the output, type = boolean;
  #' @param spatial_association_method Method used for data aggregation random method (if a fishing data is on several polygons (borders case) the polygon is chosen randomly), equal distribution method (if a fishing data is on several polygons (borders case) the fishing value are distribuated between these polygons) or cwp method (The processing attributes each geolocation to an unique polygon according to CWP rules (from FAO) (http://www.fao.org/fishery/cwp/en)) are available. Value : "random|equaldistribution|cwp", type = character;
  #' @param intersection_layer shapefile of your spatial zone with the same CRS of data if you use a irregular spatial zone (like : EEZ), type = SpatialPolygonDataframe;
  #'
  #' @return Return the dataframe containing data. If aggregate_data is TRUE, the dataframe is aggregated data by input dimensions, by space and by time. If aggregate_data is FALSE, dataframe containes input data associated with intersected spatial zones and intersected calendar periods
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords aggregation, spatio-temporal resolution, space, time, geolocalisation, dimension
  #'
  #' @usage
  #'  output_data <- geolocalisation_aggregation(df_input,grid_spatial_resolution=1,latmin=-90,latmax=90,lonmin=-180,lonmax=180,first_date="1900-01-01",final_date="20017-12-31",temporal_resolution=1,temporal_resolution_unit="m",program_observe=F, aggregate_data=T)
  #'  output_data <- geolocalisation_aggregation(df_input,grid_spatial_resolution=NULL,latmin=-90,latmax=90,lonmin=-180,lonmax=180,first_date="1900-01-01",final_date="20017-12-31",temporal_resolution=1,temporal_resolution_unit="m",program_observe=F, aggregate_data=T, intersection_layer=EEZ, label_id_geom="geom_name")


  ######################## Package
  all_packages <- c("data.table","lubridate","dplyr","sp","rgeos","rgdal","stringr")
  for(package in all_packages){
    if (!require(package,character.only = TRUE)) {
      install.packages(package)  
    }
    require(package,character.only = TRUE)
  }
  source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/create_calendar.R")
  source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/create_grid.R")
  
  cat("\n Treatments in progress :")
  
  ######################## Initialisation
  ### Convert data in a table (processing faster)
  dataset_table<- as.data.table(df_input)
  ### delete duplicate data
  dataset_table <- unique(dataset_table)
  
  ### aggragation parameters
  list_dimensions_output = setdiff(colnames(df_input),c("value","date","lat","lon"))
  var_aggregated_value = "value"
  #fact_name = agg_parameters$fact_name           ### A voir la réelle utilité pour fads et effort. Pourrait être effectué en dehors de la fonction?
  #if (fact_name =="fad"){number_days = agg_parameters$calculation_of_number_days}

  cat("\n Creation of temporal calendar ... ")
  
  ######################## Clip data by time
  ### create calendar
  if (is.null(first_date)){
  first_date<-min(dataset_table$date)
  }
  if (is.null(final_date)){
    final_date<-max(dataset_table$date)
  }
  calendar <- create_calendar(first_date,final_date,temporal_resolution,temporal_resolution_unit)
  calendar <- data.table(calendar)
  
  cat("\n Creation of temporal calendar OK ")
  cat("\n Temporal treatment in progress ... ")
  
  ### Join dataset and calendar
  ## initialisation
  dataset_faketime_s_e <- data.table(dataset_table, time_start=as_date(dataset_table$date), time_end=as_date(dataset_table$date))
  setkey(calendar, time_start, time_end) # is used in the function : foverlaps
  ## select the index calendar for each "time" of data
  index <- foverlaps(x=dataset_faketime_s_e,y=calendar, by.x = c("time_start","time_end"),by.y = c("time_start","time_end"), type="within", nomatch=0, which = T)
  ### join data and calendar
  dataset_calendar <- data.table(dataset_table[index$xid,], calendar[index$yid,])
  ### list of used period in calendar
  data_calendar <-  calendar[unique(index$yid),]
  
  cat("\n Temporal treatment in progress OK ")
  cat("\n Spatial zone creation in progress ... ")
  
  ######################## Create polygon
  ### Creates spatial polygons for data aggregation
  ### If spatial zones (spatialpolygondataframe with a label by polygons) is add by the user, these spatial zones is used
  ### If not a grid are create according to extent zone, spatial resolution choised by the user
  if (is.null(intersection_layer)){
    centred_grid=TRUE
    latmin=min(dataset_table$lat)
    latmax=max(dataset_table$lat)
    lonmin=min(dataset_table$lon)
    lonmax=max(dataset_table$lon)
    sp_zone <- create_grid(latmin,latmax,lonmin,lonmax,grid_spatial_resolution,crs=data_crs,centred=centred_grid)
  } else {
    ### spatial zone add by the user
    ## class : sp, spatialPolygonsDataframe with the label of zones
    sp_zone <- intersection_layer
    # sp_zone <- spTransform(sp_zone , CRS(data_crs) )
  }
  
  cat("\n Spatial zone creation in progress OK ")
  cat("\n Spatial treatment in progress ... ")
  
  # ## error :
  # Error in RGEOSBinPredFunc(spgeom1, spgeom2, byid, func) :
  # rgeos_binpredfunc_prepared: maximum returned dense matrix size exceeded
  # separate data in some parts
  
  ## create a "fake" column to separate the dataset into 100 parts
  n <- 100
  nr <- nrow(dataset_calendar)
  dataset_calendar<-split(dataset_calendar, rep(1:ceiling(nr/n), each=n, length.out=nr))
  
  for (i in 1:length(dataset_calendar)){
    dataset_calendar[[i]]$splt<-i
  }
  dataset_calendar<-data.frame(Reduce(rbind, dataset_calendar))

  
  unique_id = unique(dataset_calendar$splt)

  length_unique_id <- length(unique_id)
  
  output_data_detail <- NULL
  compteur=0
  
  for (id_subdata in unique_id){
  
      dataset <- subset(dataset_calendar,dataset_calendar$splt==id_subdata)

    ######################## Create spatial point
    sp_points <- SpatialPointsDataFrame(dataset[,c("lon","lat")], dataset,proj4string = CRS(data_crs))
    
    ######################## Select polygones which are covered by geolocalisation
    ### select polygons covered by geolocalisation
    join_poly <- overGeomGeom(sp_zone,gEnvelope(sp_points))
    id_polygons <- which(!is.na(join_poly))
    polygons <- sp_zone[id_polygons,]

    # if polygons=EEZ, intersection between data and polygons can be NULL
    if (length(polygons)>0){
      ######################## Intersection between points and polygons
      intersection <- gIntersection(sp_points,polygons,byid=T, drop_lower_td = T)

      # NOTE : id of a spatial polygon
      # intersect_buffer[i]@polygons[[1]]@ID
      
      ## if polygons=ZEE, data extent can intersect polygons but not real geolocation data
      if(!is.null(intersection)){
        ######################## Create data.table without data aggregation 
        ### Initialisation
        ### extract dimensions data ID, other dimensions and variable
        sp_dim_data <- str_replace(rownames(intersection@coords), " " , fixed("."))
        sp_dim_data <- str_split_fixed(sp_dim_data, fixed("."),2)
        sp_dim_data <- data.table(sp_dim_data)
        colnames(sp_dim_data) <- c("id_point","id_geom")
        
        all_dim_data <- data.table(id_point=rownames(sp_points@data),sp_points@data)#, sp_points@coords)
        
        ### Merge spatial data ID and dimensions
        sp_data <- merge(sp_dim_data,all_dim_data, by="id_point")
        
        ### extract WKT or label from polygons
        id_poly <- sapply(polygons@polygons,slot, "ID")
        if(is.null(intersection_layer)){
          sp_transform_poly <- spTransform(polygons,CRS(data_crs))
          wkt <- writeWKT(sp_transform_poly, byid = T)
          if (spatial_association_method=="cwp"){ 
            dist_0_centr_poly <- data.table(spDistsN1(gCentroid(sp_transform_poly, byid=TRUE),c(0,0),longlat=T))
            coord_centroid_geom <- data.table(gCentroid(sp_transform_poly, byid=TRUE)@coords)
            wkt <- cbind(wkt,dist_0_centr_poly,coord_centroid_geom)
          }
          poly <- data.table(id_poly,wkt,wkt)
        } else {
          wkt <- writeWKT(spTransform(gEnvelope(polygons, byid=TRUE, id = NULL),CRS(data_crs)), byid = T)
          poly <- data.table(id_poly,polygons@data$geographic_identifier,wkt)
        }
        names(poly) <- c("id_geom","geographic_identifier", "geom_wkt",
                         if(spatial_association_method=="cwp"){c("dist_0_centr_poly","lon_cent_geom", "lat_cent_geom")})
        
        ### Merge spatial data, dimensions and variable
        output_data_detail_id_with_duplicated <- merge(sp_data,poly, by="id_geom")
        output_data_detail_id_with_duplicated$id_point <- as.numeric(output_data_detail_id_with_duplicated$id_point)
        
        ### Extract the duplicated geolocalisation (points on polygon boundary)
        table_number_rep <- table(as.factor(output_data_detail_id_with_duplicated$id_point))
        ## Extract id
        # id_duplicated <- as.numeric(which(table_number_rep>1))
        id_duplicated <- names(which(table_number_rep>1))
        # id_unique <- as.numeric(which(table_number_rep==1))
        id_unique <- names(which(table_number_rep==1))
        
        ## Extract data
        duplicated_data <- output_data_detail_id_with_duplicated[which(output_data_detail_id_with_duplicated$id_point %in% id_duplicated),]
        output_data_detail_id <- output_data_detail_id_with_duplicated[which(output_data_detail_id_with_duplicated$id_point %in% id_unique),]
        
        ## Store non duplicate data
        output_data_detail <- bind_rows(output_data_detail,output_data_detail_id )
        
        ### Select the traitement for points on polygon boundary
        for ( id in id_duplicated) {
          subset_duplicated_data <- subset(duplicated_data, duplicated_data$id_point == id)
          switch (spatial_association_method,
                  "random" = {
                    id_select=data.table(rand=runif(dim(subset_duplicated_data)[1], min = 0, max = 1), keep.rownames = T)
                    select_data <- subset_duplicated_data[id_select$rand==max(id_select),] 
                  },
                  "equaldistribution" = {
                    size <- dim(subset_duplicated_data)[1]
                    select_data <- subset_duplicated_data
                    select_data[,var_aggregated_value] <- select_data[,var_aggregated_value, with=F]/size
                  }, 
                  "cwp" = {
                    if (is.null(intersection_layer)){
                      ## reste à tester lat ==0 et/ou lon ==0
                      lat <- subset_duplicated_data$lat[1]
                      lon <- subset_duplicated_data$lon[1]
                      if (lat==0 ){
                        select_data <- subset_duplicated_data[abs(subset_duplicated_data$lon_cent_geom)==max(abs(subset_duplicated_data$lon_cent_geom)) & subset_duplicated_data$lat_cent_geom>0 ,]
                      } else if (lon==0 ){
                        select_data <- subset_duplicated_data[abs(subset_duplicated_data$lat_cent_geom)==max(abs(subset_duplicated_data$lat_cent_geom)) & subset_duplicated_data$lon_cent_geom>0 ,]
                      } else if (lon==0 & lat ==0){
                        select_data <- subset_duplicated_data[subset_duplicated_data$lat_cent_geom>0 & subset_duplicated_data$lon_cent_geom>0,]
                      } else {
                        select_data <- subset_duplicated_data[subset_duplicated_data$dist_0_centr_poly==max(subset_duplicated_data$dist_0_centr_poly)]
                      }
                    } else {
                      stop("CWP can't be used on irregular spatial zone. Please to select random or equaldistribtion methods.")
                    }
                  }
          )
          
          ## Store the data selected by the method
          output_data_detail <- bind_rows(output_data_detail,select_data )
        }
        
        compteur = compteur +1
        cat(paste0("\n", compteur, " % at ", Sys.time(), " ... "))
        
        ### bbox
        if (compteur>1){
          bbox <- gUnion(bbox,polygons)
          bbox <- gEnvelope(bbox)
        } else {
          bbox <- gEnvelope(polygons)
        }
        
      } else {
        compteur = compteur +1
        cat("No intersection between the subset ",id_subdata," and spatial zone")
      }
      
    } else {
      compteur = compteur +1
      cat("No intersection between the subset ",id_subdata," and spatial zone")
    }

  }
  
  cat("\n Spatial treatment OK ")
  
  
  ## Select the wanted dimenions
  list_remove_dim <- c(if(spatial_association_method=="cwp"){c("dist_0_centr_poly","lon_cent_geom","lat_cent_geom")},c("id_point","id_geom"))
  if (dim(output_data_detail)[1]<1){
    warnings("The intersection between data and sptial zone is empty")
  } else {
    output_data_detail <- output_data_detail[,-list_remove_dim, with=F]
  }
  
  output_data_detail$splt<-NULL
  
  output_data_detail$geographic_identifier<-as.character(output_data_detail$geographic_identifier)
  output_data_detail$time_start<-as.character(output_data_detail$time_start)
  output_data_detail$time_end<-as.character(output_data_detail$time_end)
  
  
  ## Special case for facts effort and FAD
  #if (fact_name=="effort"){
    ## calculation : boats effort
  # output_data_detail <- number_boat(output_data_detail)
  #} else if (fact_name=="fad"){
    ## calculation number of days and number of fad by space and by time
  # output_data_detail <- number_fad_and_days(output_data_detail, number_days)
    ## Data are already aggregated in the function number_fad_and_days
  #}

  
  ######################## Aggregation of data
  if (aggregate_data==T){ #& fact_name!="fad"){
    
    cat("\n Aggregation of data ... ")
    # list of dimensions
    list_dimensions_output_modify <- c(list_dimensions_output,"time_start","time_end","geographic_identifier","geom_wkt")
    output_data_agg <- output_data_detail %>% select_(.dots=c(list_dimensions_output_modify,"value")) %>% group_by_(.dots=list_dimensions_output_modify) %>% dplyr::summarise_all (funs(n(),sum,mean,sd,min,max)) %>% setNames( c(list_dimensions_output_modify, "n_value","sum_value","mean_value","sd_value","min_value","max_value")) 
    output_data <- data.frame(output_data_agg) 
    cat("\n Aggregation of data OK ")
    
  } else {
    ## store data without aggregation but associate by time and by space
    output_data <- data.frame(output_data_detail)
  }

  return(output_data)
  
} 
