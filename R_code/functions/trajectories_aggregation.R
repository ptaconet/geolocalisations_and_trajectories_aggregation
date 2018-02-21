####################### List of functions
# Author : Chloé Dalleau, Geomatic engineer (IRD)
# Supervisor : Julien Barde (IRD)
# Date : 16/02/2018 
# 
# ## Summary 
# 1. trajectories_aggregation : 
#       a- aggregates trajectories by space and by time, 
#       b- calulates the distance travelled, normalize distance travelled, surface explored and normalize surface explored by each object
# 2. preprocessing_data_trajectories : format input data for the function trajectories_aggregation



trajectories_aggregation <- function(raw_dataset, buffer_size=10,spatial_reso=1,latmin=-90,latmax=90,lonmin=-180,lonmax=180,firstdate="1900-01-01",finaldate=Sys.Date(),temporal_reso=1,temporal_reso_unit="month", list_dim_output=NULL, aggregate_data=T, spatial_zone=NULL, label_id_geom=NULL){
  
  #' @name trajectories_aggregation
  #' @title Trajectories aggregation and distance/surface calulation by dimensions, by space and by time
  #' @description Take a data frame with geolocalisation dimension ("lon" for longitude and "lat" for latitude), date time dimension ("time"), objects and trajectories identifiers ("id_object" and "id_traj") and several data dimensions (like flag, gear...). Generate a spatial grid with input parameter (grid extent, spatial resolution) or take an input shapefile with spatial polygons from the package sp and "label_id_geom". Generate a continuous calendar of period (day, 1/2 month, month, year) with input parameter (calendar limits, temporal resolution and temporal resolution unit).  The algorithm associates geolocalisation data with intersect spatial zone and intersect calendar period. The algorithm creates trajectories by object identifier,by trajectories identifier,by space, by time and by input dimensions. Then some variables are calculated : distance, normalize distance, surface explored, normalize surface explored. If aggregate_data is TRUE, variables are aggregated by data dimensions, by space and by time.
  #' 
  #' @param raw_dataset dataframe with geolocalisation objects and containing: "id_object" for object identifier, "id_traj" for trajectory identifier, data dimensions,"time" for geolocalisation date time , "lat" for latitude of the position in degree, "lon"  for longitude of the position in degree, type = data.frame;
  #' @param buffer_size size of the buffer around the trajectory to calculate the surface exploited (unit:km), type=integer;
  #' @param spatial_reso spatial resolution of the grid in degree, type = integer;
  #' @param latmin smallest latitude for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param latmax biggest latitude wanted for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param lonmin smallest longitude for the spatial grid in degree (range: -180 to 180), type = integer;
  #' @param lonmax biggest longitude wanted for the spatial grid in degree (range: -180 to 180), type = integer;
  #' @param firstdate first date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param finaldate final date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param temporal_reso temporal resolution of the calendar in day, month or year (see: temporal_reso_unit). Note: for 1/2 month put temporal_reso=1/2 and temporal_reso_unit="month" , type = integer;
  #' @param temporal_reso_unit temportal resolution unit od calendar, accepted value : "day" "month" "year", type = character;
  #' @param list_dim_output dimensions list from input data and which will appear in the output data, type = character;
  #' @param aggregate_data Put TRUE if you want aggregated data in the output, type = boolean;
  #' @param spatial_zone shapefile of your spatial zone with the same CRS of data if you use a irregular spatial zone (like : EEZ), type = SpatialPolygonDataframe;
  #' @param label_id_geom label use for spatial geometry in your shapefile if you use a irregular spatial zone (like : EEZ), type = character;
  #'
  #' @return Return a list with (1) the dataframe containing data and (2) a list of metadata. If aggregate_data is TRUE, the dataframe is aggregated data by input dimensions, by space and by time. If aggregate_data is FALSE, dataframe containes input data associated with intersected spatial zones and intersected calendar periods. The calculated variable are :  distance : total distance for all objects in each cell of the grid for a time period in km, ndistance : normalize total distance for all objects in each cell of the grid for a time period, surface : total surface exploited for all objects in each cell of the grid for a time period in km², nsurface : normalize total surface exploited for all objects in each cell of the grid for a time period in km², number_of_trajectories : number object aggregated in each cell of the grid for a time period
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords trajectory, aggregation, spatio-temporal resolution, distance, noralize distance, surface explored, normalize surface explored
  #'
  #' @usage output <- trajectories_aggregation(dataset,buffer_size,spatial_reso,latmin,latmax,lonmin,lonmax,firstdate=firstDate,finaldate=finalDate,temporal_reso,temporal_reso_unit,aggregate_data, list_dim_output=list_dim_output,spatial_zone=spatial_zone ,label_id_geom=label_id_geom)
  

  ######################## Package
  require(data.table)
  require(sp)
  require(lubridate)
  require(rgeos)
  require(dplyr)
  require(stringr)
  require(raster)
  source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/create_calendar.R")
  source("https://raw.githubusercontent.com/cdalleau/geolocalisations_and_trajectories_aggregation/master/R_code/functions/create_grid.R")
  
  
  cat("\n Treatments in progress :")
  
  ######################## Initialisation
  ### Converts data in a table (processing faster)
  dataset_table<- as.data.table(raw_dataset)
  ### Delates duplicate data
  dataset_table <- unique(dataset_table)
  ### Sorts data by time
  dataset_table <- setorder(dataset_table, time)
  
  
  ######################## Select points in extent zone 
  dataset_table <- dataset_table[between(lon, lonmin,lonmax) & between(lat, latmin,latmax)]
  
  
  ######################## Merge data and a calendar (based on temporal resolution choised) 
  ### Creates calendar
  calendar <- create_calendar(firstdate=firstdate,finaldate=finaldate,temporal_reso,temporal_reso_unit)
  calendar <- data.table(calendar)
  
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
  ### release space memory
  rm(dataset_faketime_s_e)
  
  ######################## Create polygon
  ### Creates spatial polygons for data aggregation
  ### If spatial zones (spatialpolygondataframe with a label by polygons) is add by the user, these spatial zones is used
  ### If not a grid are create according to extent zone, spatial resolution choised by the user
  if (is.null(spatial_zone)){
    ### Creates polygons grid
    centred_grid=FALSE
    sp_zone <- create_grid(latmin=latmin,latmax=latmax,lonmin=lonmin,lonmax=lonmax,spatial_reso,crs=data_crs,centred=centred_grid)
  } else {
    ### spatial zone add by the user
    ## class : sp, spatialPolygonsDataframe with the label of zones
    sp_zone <- spatial_zone
  }
  
  ### Convert grid crs to projected crs
  ## Note : keep grid creation in data_crs and after do spTransform
  data_crs_proj <- "+init=epsg:3395"
  sp_zone <- spTransform(sp_zone , CRS(data_crs_proj) )
  
  ### Error Rgeos :
  ## Error in RGEOSBinPredFunc(spgeom1, spgeom2, byid, func) :
  ## rgeos_binpredfunc_prepared: maximum returned dense matrix size exceeded
  ## solution:  separate data by id_object
  #   
  #   dataset_calendar$id_object <- as.factor(dataset_calendar$id_object)
  unique_idobject <- unique(dataset_calendar$id_object)
  
  ### Initialisation of final data
  output_data_detail <- NULL
  ## dimensions list to split data
  list_dim <- setdiff(names(dataset_calendar), c("time", "lat", "lon"))
  compteur = 0
  
  for (id in unique_idobject){
    
    ######################## Split data in a list according to dimensions
    ### Extract data for the part id_part
    dataset <- filter(dataset_calendar,id_object==id)
    
    ### If the object have more than 1 geolocalisation, the trajectories are calculated
    if (dim(dataset)[1]>1){
      ### Split data
      # split_data_by_time <- split(dataset, dataset[,list_dim, with=FALSE], drop= T) 
      split_data_by_time <- split(dataset, dataset[,list_dim], drop= T) 
      
      ### Select data with more than 1 point
      list_data_by_time <- split_data_by_time[which(sapply(split_data_by_time,dim)[1,]>1)]
      
      if (length(list_data_by_time)>0){
        
        ######################## Create lines trajectories
        ### Create the geometry "line" for each object : Line-class will be used in Lines-class
        trajectories <- list()
        # coord <- lapply(list_data_by_time, "[", j=c("lon","lat"), with=F)
        coord <- lapply(list_data_by_time, "[", c("lon","lat"))
        list_line <- lapply(coord, Line)
        ### create the geometry "lines" for each object: Lines-class will be used in SpatialLines
        ### the loup is obligatory to create the IDs
        for (i in 1:length(list_line)){
          trajectories[[i]] <- Lines(list_line[i], ID=names(list_line[i])) 
        }
        ### Convert Lines to SpatialLines
        sp_trajectories <- SpatialLines(trajectories, proj4string = CRS(data_crs))
        ### release space memory
        rm(trajectories)
        sp_trajectories <- spTransform(sp_trajectories , CRS( data_crs_proj ) )
        ### Creat a buffer arround the lines for surface calculation
        ### ERROR memory space : alternative solution is a loop
        # buffer_sp_traj <- gBuffer(sp_trajectories, capStyle = "ROUND", joinStyle = "ROUND", byid = T, width = buffer_size*1000)
        cat(paste0("\n buffer step (",length(sp_trajectories),") :"))
        count=0
        for (i in 1:length(sp_trajectories)){ 
          traj <- gSimplify(sp_trajectories[i],tol=0.00001))
          if (gIsValid(traj)==T){
            buffer <- gBuffer(traj, capStyle = "ROUND", joinStyle = "ROUND", width = buffer_size*1000)
            count=count+1
            if (count==1){
                buffer_sp_traj <- buffer
            } else {
                buffer_sp_traj <- union(buffer_sp_traj,buffer)
            }
             ### release space memory
            rm(buffer)
            cat(paste0(count," "))
          } 
        }

        
        # ### test : plot trajectoire
        # # id = unique_idobject[4]
        # plot(sp_trajectories, axes=T)
        # sp_points <- SpatialPointsDataFrame(dataset[,c("lon","lat")], dataset, proj4string = CRS(data_crs))
        # points(sp_points, col="red")
        # row <- which(sp_points@data["lat"]>(20))
        # sp_points@data[(row-10):(row+10),]
        # dataset_sql <- data.table(dataset_sql)
        # dataset_sql[which(id_object==19 & id_traj==14 & lat<20),]


        ######################## Select polygones which are covered by trajectories
        ### select polygons covered by trajectories
        join_poly <- overGeomGeom(sp_zone,buffer_sp_traj)
        id_polygons <- which(!is.na(join_poly))
        polygons <- sp_zone[id_polygons,]
        

        
        # if polygons=ZEE, the intersection between sp_zone and the trajectories can be NULL
        if (length(polygons)>0){
          ######################## Clip trajectories and buffers by poly
          ### Intersection between trajectories and polygons
          intersect_traj <- gIntersection(sp_trajectories,polygons,byid=T, drop_lower_td = T)
          ### Intersection between buffer and polygons
          intersect_buffer <- gIntersection(buffer_sp_traj,polygons, byid = T,  drop_lower_td = T)
          
          
          # NOTE : id of a spatial polygon
          # intersect_buffer[i]@polygons[[1]]@ID
          
          ######################## Calculate variables : distance and buffer surface
          ### Calculate the distance by objects and by polygons
          ## Note : keep if, if geolocatisations are fixed gIntersection can create NULL data
          if(!is.null(intersect_traj)){
            distance <- as.data.table(round(gLength(intersect_traj,byid=T)/1000,3), keep.rownames=T)
            colnames(distance) <- c("id","distance")
          }
          ### Calculate the surface exploited by objects and by polygons
          surface <- as.data.table(round(gArea(intersect_buffer,byid=T)/1000000,3), keep.rownames=T)
          colnames(surface) <- c("id","surface")
          
          
          ######################## Merge all the calculated variables
          var_cal <- merge(distance,surface, by="id",all= T)
          var_cal[is.na(var_cal)] <- 0
          
          
          ######################## Create output data without data aggregation 
          ### Initialisation
          ### extract dimensions data
          dim_data <- str_replace(var_cal$id, " " , fixed("."))
          dim_data <- str_split_fixed(dim_data, fixed("."),length(list_dim)+1)
          dim_data <- data.table(dim_data)
          colnames(dim_data) <- c(list_dim, "id_geom")
          
          ### extract WKT or label from polygons
          id_poly <- sapply(polygons@polygons,slot, "ID")
          if(is.null(spatial_zone)){
            wkt <- writeWKT(spTransform(polygons,CRS(data_crs)), byid = T)
            poly <- data.table(id_poly,wkt)
          } else {
            wkt <- writeWKT(spTransform(gEnvelope(polygons, byid=TRUE, id = NULL),CRS(data_crs)), byid = T)
            poly <- data.table(id_poly,polygons@data[label_id_geom],wkt)
          }
          names(poly) <- c("id_geom",if(is.null(spatial_zone)==FALSE){"geographic_identifier_label"}, "geom_wkt" )
          
          
          ### Bind data and calculate variables
          output_data <-  as.data.table(cbind(dim_data,var_cal[,-"id"]))
          
          ### Merge data and polygons
          output_data <- merge(poly,output_data, by="id_geom")
          
          
          compteur = compteur +1
          cat(paste0("\n           ",compteur," object(s) trajectories created."))
          
          ### Store data for the loop
          output_data_detail <- data.table(rbind(output_data_detail,output_data))
          
          ### bbox
          if (compteur>1){
            bbox <- gUnion(bbox,polygons)
            bbox <- gEnvelope(bbox)
          } else {
            bbox <- gEnvelope(polygons)
          }
          ### release space memory
          rm(polygons)
          rm(output_data)
          
        }
        
      }
      
    }
    
    
  }
  

  
  ### Calulate normalize data
  output_data_detail$ndistance <- sapply(output_data_detail[,"distance"], function(x) (x-min(x))/(max(x)-min(x)))
  output_data_detail$nsurface <- sapply(output_data_detail[,"surface"], function(x) (x-min(x))/(max(x)-min(x)))
  
  
  ######################## Aggregation of data
  if (aggregate_data==T){
    
    cat("\n Aggregation of data in progress ... ")
    
    ######################## Aggregation of data
    ### list of dimensions for the output
    dimensions <-c("geom_wkt", if(is.null(spatial_zone)==FALSE){"geographic_identifier_label"}, "time_start", "time_end", list_dim_output)
    
    ### Aggregation (data.table package)
    output_data_agg <- output_data_detail %>% group_by_(.dots=dimensions) %>% summarise(distance=round(sum(distance),3),surface=round(sum(surface),3), number_of_trajectories=n())# summarise(distance=round(sum(distance),3),surface=round(sum(surface),3) )
    output_data_agg <- data.table(output_data_agg)
    ### Calculate normalize data
    ndistance <- (output_data_agg[,"distance"]-min(output_data_agg[,"distance"]))/(max(output_data_agg[,"distance"])-min(output_data_agg[,"distance"]))
    colnames(ndistance)<-"ndistance"
    nsurface<- (output_data_agg[,"surface"]-min(output_data_agg[,"surface"]))/(max(output_data_agg[,"surface"])-min(output_data_agg[,"surface"]))
    colnames(nsurface)<-"nsurface"
    ## Bind all data
    
    output_data <- data.table(output_data_agg[,dimensions, with=FALSE], output_data_agg[,"distance"],
                              ndistance, output_data_agg[,"surface"],
                              nsurface, output_data_agg[,"number_of_trajectories"] 
    )
    cat("ok \n")
    
  } else {
    ### Store data without agregation
    output_data <- output_data_detail[,-"id_geom"]
  }
  # 

  
  ### output
  # list of data, main metadata
  # extract spatial extent from data
  bbox <- extent(spTransform(bbox, CRS(data_crs)))
  bbox_extent <- paste("lon:", format(bbox[1], digits=2), format(bbox[2], digits=2)," lat:", format(bbox[3], digits=2), format(bbox[4], digits=2))
  
  summary <- paste("This file is a time series of", object_type, "trajectories and contains : distance, explored surface and the number of trajectories.",sep=" ")
  title <-  paste("Aggregation of ", object_type, " trajectories by time (from ", firstdate," to ", finaldate," with a resolution of ", temporal_reso, temporal_reso_unit,") and by space (", bbox_extent ," with a resolution of ",  spatial_reso,"deg",")", sep = "")
  date <- as.character(Sys.time())
  lineage <- paste0("step1: Through a dedicated R script,", object_type," trajectories was created based on geo-localize points. Then the trajectories distance, explored surface and the number of trajectories are calculed by time and by space. These data are aggregated by time and by space. step2:  The dataset was exported in CSV.")
  spatial_resolution <-  spatial_reso
  buffer_size <- buffer_size
  metadata <- list(title, date,spatial_resolution,summary, lineage, buffer_size)
  names(metadata) <- c( "title", "date", "spatial_resolution", "summary", "lineage", "buffer_size")
  
  output <- list(output_data, metadata)
  names(output) <- c("data", "metadata_list")
  
  return(output)
} 


preprocessing_data_trajectories <- function(dataset,list_dim_output,colname_idobject, colname_idtraj,colname_time,colname_lat,colname_lon){

  #' @name preprocessing_data_trajectories
  #' @title Format input data for the function trajectories_aggregation
  #' @description Format input data for the function trajectories_aggregation : changes column names with right column names for function trajectories_aggregation
  #' 
  #' @param dataset dataframe with geolocalisation objects and containing: object identifier, trajectory identifier, data dimensions,geolocalisation date time , latitude of the position in degree, for longitude of the position in degree, type = data.frame;
  #' @param list_dim_output dimensions list from input data and which will appear in the output data, type = character;
  #' @param colname_idobject column name for object identifier in the input data, type = character;
  #' @param colname_idtraj column name for trajectory identifier in the input data, type = character;
  #' @param colname_time column name for time in the input data, type = character;
  #' @param colname_lat column name for latitude in the input data, type = character;
  #' @param colname_lon column name for longitude in the input data, type = character;
  #'
  #' @return Return a dataframe with the rigth column name for the function trajectories_aggregation, and remove the unused columns 
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords preprocessing, aggregation, spatio-temporal resolution
  #'
  #' @usage dataset <- preprocessing_data_trajectories(dataset=dataset_sql,list_dim_output,colname_idobject, colname_idtraj,colname_time,colname_lat,colname_lon)

  colnames(dataset)[colnames(dataset)==colname_time] <- "time"
  colnames(dataset)[colnames(dataset)==colname_idobject] <- "id_object"
  colnames(dataset)[colnames(dataset)==colname_lat] <- "lat"
  colnames(dataset)[colnames(dataset)==colname_lon] <- "lon"
  colnames(dataset)[colnames(dataset)==colname_idtraj] <- "id_traj"
  
  dataset$id_traj <- as.numeric(dataset$id_traj)
  
  list_dim_ <- unique(c(list_dim_output,"id_object", "id_traj","time", "lat", "lon"))
  dataset <- dataset[,list_dim_]
  
  return(dataset)
}
