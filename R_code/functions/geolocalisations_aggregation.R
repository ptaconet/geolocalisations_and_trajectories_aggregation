####################### List of functions
# Author : Chloé Dalleau, Geomatic engineer (IRD)
# Supervisor : Julien Barde (IRD)
# Date : 15/02/2018 
# 
# ## Summary 
# 1. geolocation_aggregation: aggregates data by dimensions, by space and by time
# 2. number_boat: calculate number of boats for effort fact by dimensions, by space and by time
# 3. number_fad_and_days : calculate and aggregate the number of FAD and days by dimensions, by space and by time


geolocalisations_aggregation <- function(raw_dataset,spatial_reso=1,latmin=-90,latmax=90,lonmin=-180,lonmax=180,data_crs="+init=epsg:4326 +proj=longlat +datum=WGS84" ,firstdate="1900-01-01",finaldate=Sys.Date(),temporal_reso=1,temporal_reso_unit="month",program_observe=F, aggregate_data=F,method_asso="equaldistribution",aggregation_parameters=NULL,spatial_zone=NULL, label_id_geom=NULL){
  
  #' @name geolocation_aggregation
  #' @title Geolocation aggregation by dimensions, by space and by time
  #' @description Take a data frame with geolocalisation dimension ("lon" for longitude and "lat" for latitude), date time dimension ("time"), several data dimensions (like flag, gear...), a variable to aggregate (like catch, effort, catch at size). Generate a spatial grid with input parameter (grid extent, spatial resolution) or take an input shapefile with spatial polygons from the package sp and "label_id_geom". Generate a continuous calendar of period (day, 1/2 month, month, year) with input parameter (calendar limits, temporal resolution and temporal resolution unit). The algorithm associates input data with intersect spatial zone and intersect calendar period. When geolocalisation data are on spatial zone boundaries, 3 methods are available: (1) "equaldistribution": variable is distributed between spatial zones (equal share); (2) "random": spatial zone is chosen randomly from intersected spatial zones;  (3) "cwp": Coordinating Working Party on fishery Statistics rules establish by the Food and Agriculture Organization of the United Nations FAO. (for more information: http://www.fao.org/fishery/cwp/en). If aggregate_data is TRUE, aggregate input data by data dimensions, by space and by time. if not, the algorithm associates input data with intersect spatial zone and intersect calendar period.   
  #' @param raw_dataset dataframe with geolocalisation objects and containing: object identifier, data dimensions,"time" for geolocalisation date time , "lat" for latitude of the position in degree, "lon"  for longitude of the position in degree, type = data.frame;
  #' @param spatial_reso spatial resolution of the grid in degree, type = integer;
  #' @param latmin smallest latitude for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param latmax biggest latitude wanted for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param lonmin smallest longitude for the spatial grid in degree (range: -180 to 180), type = integer;
  #' @param lonmax biggest longitude wanted for the spatial grid in degree (range: -180 to 180), type = integer;
  #' @param data_crs a character string of projection arguments of data. The arguments must be entered exactly as in the PROJ.4 documentation, value="+init=epsg:4326 +proj=longlat +datum=WGS84", type = character;
  #' @param firstdate first date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param finaldate final date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param temporal_reso temporal resolution of the calendar in day, month or year (see: temporal_reso_unit). Note: for 1/2 month put temporal_reso=1/2 and temporal_reso_unit="month" , type = integer;
  #' @param temporal_reso_unit temportal resolution unit od calendar, accepted value : "day" "month" "year", type = character;
  #' @param program_observe Put TRUE if you want the dimension program (observe database) in the output, type = boolean;
  #' @param aggregate_data Put TRUE if you want aggregated data in the output, type = boolean;
  #' @param method_asso Method used for data aggregation random method (if a fishing data is on several polygons (borders case) the polygon is chosen randomly), equal distribution method (if a fishing data is on several polygons (borders case) the fishing value are distribuated between these polygons) or cwp method (The processing attributes each geolocation to an unique polygon according to CWP rules (from FAO) (http://www.fao.org/fishery/cwp/en)) are available. Value : "random|equaldistribution|cwp", type = character;
  #' @param aggregation_parameters if aggregate_data is TRUE list of "list_dimensions_output": the list of dimensions from input data.frame and for the output data, "var_aggregated_value": colname used in the input dataframe for the variable which will be aggregate, "sub_datasaet": colname used in the input dataframe for subset the input data (the function can create error memory if the input data are too large) if no corresponding column put NA , "fact_name": name of the fact (value: "catch", "catch_at_size", "effort", "fad"), "calculation_of_number_days" boolean to indicate if the number of day (by dimensions, space and time) is calculated (only for FAD), type=list
  #' @param spatial_zone shapefile of your spatial zone with the same CRS of data if you use a irregular spatial zone (like : EEZ), type = SpatialPolygonDataframe;
  #' @param label_id_geom label use for spatial geometry in your shapefile if you use a irregular spatial zone (like : EEZ), type = character;
  #'
  #' @return Return a list with (1) the dataframe containing data and (2) a list of metadata. If aggregate_data is TRUE, the dataframe is aggregated data by input dimensions, by space and by time. If aggregate_data is FALSE, dataframe containes input data associated with intersected spatial zones and intersected calendar periods
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords aggregation, spatio-temporal resolution, space, time, geolocalisation, dimension
  #'
  #' @usage
  #'  output_data <- geolocalisation_aggregation(raw_dataset,spatial_reso=1,latmin=-90,latmax=90,lonmin=-180,lonmax=180,firstdate="1900-01-01",finaldate="20017-12-31",temporal_reso=1,temporal_reso_unit="m",program_observe=F, aggregate_data=T)
  #'  output_data <- geolocalisation_aggregation(raw_dataset,spatial_reso=NULL,latmin=-90,latmax=90,lonmin=-180,lonmax=180,firstdate="1900-01-01",finaldate="20017-12-31",temporal_reso=1,temporal_reso_unit="m",program_observe=F, aggregate_data=T, spatial_zone=EEZ, label_id_geom="geom_name")


  ######################## Package
  all_packages <- c("data.table","lubridate","dplyr","sp","rgeos","rgdal","stringr","rlang")
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
  dataset_table<- as.data.table(raw_dataset)
  ### delate duplicate data
  dataset_table <- unique(dataset_table)
  
  ### aggragation parameters
  list_dimensions_output = agg_parameters$list_dimensions_output
  var_aggregated_value = agg_parameters$var_aggregated_value
  sub_dataset = agg_parameters$sub_dataset
  fact_name = agg_parameters$fact_name
  if (fact_name =="fad"){number_days = agg_parameters$calculation_of_number_days}
  
  ######################## Select points in extent zone 
  dataset_table <- dataset_table[between(lon, lonmin,lonmax) & between(lat, latmin,latmax)]
  
  cat("\n   Temporal calendar creation in progress ... ")
  
  ######################## Clip data by time
  ### create calendar
  calendar <- create_calendar(firstdate,finaldate,temporal_reso,temporal_reso_unit)
  calendar <- data.table(calendar)
  
  cat(" ok ")
  cat("\n   Temporal treatment in progress ... ")
  
  ### Join dataset and calendar
  ## initialisation
  dataset_faketime_s_e <- data.table(dataset_table, time_start=as_date(dataset_table$time), time_end=as_date(dataset_table$time))
  setkey(calendar, time_start, time_end) # is used in the function : foverlaps
  ## select the index calendar for each "time" of data
  index <- foverlaps(x=dataset_faketime_s_e,y=calendar, by.x = c("time_start","time_end"),by.y = c("time_start","time_end"), type="within", nomatch=0, which = T)
  ### join data and calendar
  dataset_calendar <- data.table(dataset_table[index$xid,], calendar[index$yid,])
  ### list of used period in calendar
  data_calendar <-  calendar[unique(index$yid),]
  
  cat(" ok ")
  cat("\n   Spatial zone creation in progress ... ")
  
  ######################## Create polygon
  ### Creates spatial polygons for data aggregation
  ### If spatial zones (spatialpolygondataframe with a label by polygons) is add by the user, these spatial zones is used
  ### If not a grid are create according to extent zone, spatial resolution choised by the user
  if (is.null(spatial_zone)){
    centred_grid=TRUE
    sp_zone <- create_grid(latmin,latmax,lonmin,lonmax,spatial_reso,crs=data_crs,centred=centred_grid)
  } else {
    ### spatial zone add by the user
    ## class : sp, spatialPolygonsDataframe with the label of zones
    sp_zone <- spatial_zone
    # sp_zone <- spTransform(sp_zone , CRS(data_crs) )
  }
  
  cat(" ok ")
  cat("\n   Spatial treatment in progress : ")
  
  # ## error :
  # Error in RGEOSBinPredFunc(spgeom1, spgeom2, byid, func) :
  # rgeos_binpredfunc_prepared: maximum returned dense matrix size exceeded
  # separate data in some parts
  if (!is.na(sub_dataset)){
    unique_id = unique(dataset_calendar[[sub_dataset]])
  } else {
    unique_id = 1
  }
  length_unique_id <- length(unique_id)
  
  output_data_detail <- NULL
  compteur=0
  
  for (id_subdata in unique_id){
    
    if (!is.na(sub_dataset)){
      dataset <- subset(dataset_calendar,dataset_calendar[[sub_dataset]]==id_subdata)
    } else {
      dataset <- dataset_calendar
    }
    
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
        if(is.null(spatial_zone)){
          sp_transform_poly <- spTransform(polygons,CRS(data_crs))
          wkt <- writeWKT(sp_transform_poly, byid = T)
          if (method_asso=="cwp"){ 
            dist_0_centr_poly <- data.table(spDistsN1(gCentroid(sp_transform_poly, byid=TRUE),c(0,0),longlat=T))
            coord_centroid_geom <- data.table(gCentroid(sp_transform_poly, byid=TRUE)@coords)
            wkt <- cbind(wkt,dist_0_centr_poly,coord_centroid_geom)
          }
          poly <- data.table(id_poly,wkt)
        } else {
          wkt <- writeWKT(spTransform(gEnvelope(polygons, byid=TRUE, id = NULL),CRS(data_crs)), byid = T)
          poly <- data.table(id_poly,polygons@data[label_id_geom],wkt)
        }
        names(poly) <- c("id_geom",if(is.null(spatial_zone)==FALSE){"geographic_identifier_label"}, "geom_wkt",
                         if(method_asso=="cwp"){c("dist_0_centr_poly","lon_cent_geom", "lat_cent_geom")})
        
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
          switch (method_asso,
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
                    if (is.null(spatial_zone)){
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
        cat(paste0("\n             ", compteur, " object(s) treated over",length_unique_id,"."))
        
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
  
  ## Select the wanted dimenions
  list_remove_dim <- c(if(method_asso=="cwp"){c("dist_0_centr_poly","lon_cent_geom","lat_cent_geom")},c("id_point","id_geom"))
  if (dim(output_data_detail)[1]<1){
    warnings("The intersection between data and sptial zone is empty")
  } else {
    output_data_detail <- output_data_detail[,-list_remove_dim, with=F]
  }
  
  ## Special case for facts effort and FAD
  if (fact_name=="effort"){
    ## calculation : boats effort
    output_data_detail <- number_boat(output_data_detail)
  } else if (fact_name=="fad"){
    ## calculation number of days and number of fad by space and by time
    output_data_detail <- number_fad_and_days(output_data_detail, number_days)
    ## Data are already aggregated in the function number_fad_and_days
  }

  
  ######################## Aggregation of data
  if (aggregate_data==T & fact_name!="fad"){
    
    cat("\n Aggregation of data in progress ... ")
    # list of dimensions
    list_dimensions_output_modify <- c("geom_wkt", "time_start", "time_end",list_dimensions_output)

    #sym :take strings as input and turn them into symbols (library rlang )
    output_data_agg <- output_data_detail %>% group_by_(.dots=list_dimensions_output_modify) %>% summarise(sum=sum(!!sym(var_aggregated_value)), number=n()) %>% setNames( c(list_dimensions_output_modify, var_aggregated_value, "number_of_points"))
    
    output_data <- data.table(output_data_agg) 
    list_dimensions_output_metadata <- list_dimensions_output_modify
    cat("ok")
    
  } else {
    ## store data without aggregation but associate by time and by space
    output_data <- output_data_detail
    list_dimensions_output_metadata <- list_dimensions_output
  }
  

  ######################## Create metadata 
  # extract spatial extent from data
  bbox <- bbox(spTransform(bbox, CRS(data_crs)))
  bbox_extent <- paste("lon:", format(bbox[1], digits=2), format(bbox[2], digits=2)," lat:", format(bbox[3], digits=2), format(bbox[4], digits=2))
  date <- as.character(Sys.time())
  min_date <- as_date(min(output_data$time_start))
  max_date <- as_date(max(output_data$time_end))
  start_year <- year(min_date)
  final_year <- year(max_date)
  if(!(is.null(spatial_reso))){
  spatial_resolution <-  round(spatial_reso,digits = 2)
  } else {
    spatial_resolution <- ""
  }
  temporal_resolution <- temporal_reso
  temporal_resolution_unit <- temporal_reso_unit

  if (is.null(spatial_zone)){
    step_3 <- paste0("step3: A regular grid composed of square polygons was created. The spatial extent is ",bbox_extent," with a resolution of ", spatial_resolution," decimal degrees.")
  } else {
    step_3 <- paste0("step3: Spatial data are extract from a input shapefile.")
  }
  
  switch(method_asso,
    "equaldistribution" = {step_6 = "The processing  attributes each geolocation to one or several polygons. If a fishing data is on several polygons (borders case) the fishing value are distribuated between these polygons."},
    "random" = {step_6 = "The processing attributes each geolocation to an unique polygon. If the location is between several polygons then the polygon is choice randomly."},
    "cwp" = {step_6 = "The processing attributes each geolocation to an unique polygon according to CWP rules (http://www.fao.org/fishery/cwp/en)."}
  )
  summary <- paste0(step_6,"Concerning calendar, time_start and time_end are inclusive. If there are no set in a polygon for a period, this level are not in this dataset.")
  
  lineage <- paste0("step1: ",fact_name," data from Ob7 database were collated and harmonized.
                    step2: Only data included in ",bbox_extent," from ",min_date," to ",max_date," are used. 
                    ",step_3,"
                    step4: A continius calendar was created from ",min_date," to ",max_date," with a period of ",temporal_resolution," ",temporal_resolution_unit,"(s) . The time start and time end are inclusive.
                    step5: Each fishing data was associated to one polygon using the data geolocalisation.
                    step6:",step_6,"
                    step7: Data were aggregated according to :",paste0(list_dimensions_output_metadata, collapse=", "),".
                    step8: Metadata were created according to the input data and the source database.")
  
  metadata <- list(summary ,date, lineage, spatial_resolution, start_year, final_year, temporal_resolution,temporal_resolution_unit )
  names(metadata) <- c( "summary", "date", "lineage", "spatial_resolution","start_year","final_year", "temporal_resolution","temporal_resolution_unit")
  
  output <- list(output_data, metadata)
  names(output) <- c("data", "metadata_list")
  
  return(output)
  
} 


number_boat <- function(input_dataset = output_data_detail){
  
  #' @name number_boat
  #' @title Calculation of number of boat by dimenions, by space and by time
  #' @description Calculate the varible number of boats by dimensions, by space and by time. 
  #' 
  #' @param input_dataset dataframe with fine data (no aggregated data) and effort dimensions (minimum dimensions: "c_bat" (boat identifier) 
  #' "schooltype" (type of school) "settype" (type of set) "effortunit" (effort unit) "v_effort" (effort value)), type = data.frame;
  #'
  #' @return return a dataframe (fine data) with input data and boats effort
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords aggregation, spatio-temporal resolution, boat, effort, space, time
  #'
  #' @examples
  #' output_data <- aggregation_tajectories(input_dataset=dataset)

  ### list dimensions to calculate boats effort
  ## "time","lon","lat","schooltype" ,"settype", "effortunit","v_effort" are removed
  list_dim <- setdiff(colnames(input_dataset),c("time","lon","lat","schooltype" ,"settype", "effortunit","v_effort"))
  ### select the unique boats per dimensions (exemple of dimensions: period of time, flag, gear, spatial zone)
  data_cbat <- input_dataset %>% distinct_(.dots=list_dim) 
  
  ### add effort dimensions for boats
  data_cbat_effort<- cbind(data_cbat,rep("ALL"),rep("ALL"),rep("BOATS"), rep(as.numeric(1)))
  colsize <- dim(data_cbat_effort)[2]
  setnames(data_cbat_effort, c((colsize-3):colsize), c("schooltype" ,"settype", "effortunit","v_effort"))
  
  ### merge input data and boats effort
  output_data_detail <- merge(input_dataset,data_cbat_effort, all=T)
  
  return(output_data_detail)
}

number_fad_and_days <- function(input_data, number_days=T){
  
  #' @name number_fad_and_days
  #' @title Calculation of number of FAD by dimenions, by space and by time
  #' @description If number_days = TRUE, calculate the dimension number of day by FAD, by dimensions, by space and by period of time. Calculate the varible number of FAD by dimensions, by space and by time. Aggregated data by dimensions, by space and by time
  #' 
  #' @param input_dataset dataframe with fine data (no aggregated data) and FAD dimensions (minimum dimensions: "fad_id" (FAD identifier) 
  #' "time_start" (type of school) "time_end" (type of set) "geom_wkt" (effort unit) "v_effort" (effort value)), type = data.frame;
  #' @param number_days put TRUE to calculate the dimension number of day by FAD, by dimensions, by space and by period of time, type=boolean;
  #' 
  #' @return a dataframe with aggregated data for FAD by dimensions, by space and by time
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #' @keywords aggregation, spatio-temporal resolution, FAD, space, time
  #'
  #' @examples
  #' output_data <- aggregation_tajectories(input_dataset=dataset, number_days=T)
  
  ### list dimensions to calculate number of days and number of FAD
  list_dim <- setdiff(colnames(input_data),c("lon","lat" ,"section", "pos_id"))
  
  ### select the unique FAD per dimensions
  input_data$time <- as_date(input_data$time)
  data_fad_id <- input_data %>% distinct_(.dots=list_dim) 
  
  ### add number of days
  if (number_days==T){
    ### aggregation for days calculation
    list_dim_for_days <- setdiff(colnames(data_fad_id),c("time","number_of_days"))
    data_fad_id_days <- data_fad_id %>% group_by_(.dots=list_dim_for_days) %>% summarise(number=n()) %>% setNames( c(list_dim_for_days, "number_of_days"))
    
    data_fad_id <- data.table(data_fad_id_days)
  }

  cat("\n Aggregation of data in progress ... ")
  
  ### aggregation for number of fad calculation
  list_dim_for_nfad <- setdiff(colnames(data_fad_id),c("fad_id","time"))
  data_fad <- data_fad_id %>% group_by_(.dots=list_dim_for_nfad) %>% summarise(number=n()) %>% setNames( c(list_dim_for_nfad, "number_of_fad"))
  
  data_fad <- data.table(data_fad)
  # add effort dimensions for boats
  
  cat(" ok")
  output_data_detail <- data_fad
  
  
  return(output_data_detail)
}
