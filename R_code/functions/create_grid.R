####################### List of functions
# Author : Chloé Dalleau, Geomatic engineer (IRD)
# Supervisor : Julien Barde (IRD)
# Date : 16/02/2018 
# 
# ## Summary 
# 1. create_grid : create a continious grid comprised of spatial polygons (square)

create_grid <- function(latmin,latmax,lonmin,lonmax,spatial_reso,crs,centred=T){
  #' @name create_grid
  #' @title Create continious grid 
  #' @description Create a continious grid comprised of spatial polygons (square). The algorithm uses sp package
  #' 
  #' @param spatial_reso spatial resolution of the grid in degree, type = float;
  #' @param latmin smallest latitude for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param latmax biggest latitude wanted for the spatial grid in degree (range: -90 to 90), type = integer;
  #' @param lonmin smallest longitude for the spatial grid in degree (range: -180 to 180), type = integer;
  #' @param lonmax biggest longitude wanted for the spatial grid in degree (range: -180 to 180), type = integer;  
  #' @param crs a character string of projection arguments; the arguments must be entered exactly as in the PROJ.4 documentation, type=character;
  #' @param centred put TRUE for a grid centred on zero.
  #'
  #' @return Return an object SpatialPolygon (package : sp) based on input parameters and composed by square polygons.
  #'
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords continious grid, spatial, polygons, degree
  #'
  #' @usage
  #'  grid <- create_grid(latmin=-90,latmax=90,lonmin=-180,lonmax=80,spatial_reso=1,centred=T)
  
  ### Package 
  if(!require(sp)){
    install.packages(sp)
  }
  require(sp)
  
  ### Creates polygons grid
  ## if lat and lon are equal to [-90,90] and [-180,180], it create error in spTransform (infinite value) in the code part : "Select polygones which are covered by trajectories"
    if (latmin== -90){latmin=latmin+spatial_reso}
    if (latmax== 90){latmax=latmax-spatial_reso}
    if (lonmin== -180){lonmin=lonmin+spatial_reso}
    if (lonmax== 180){lonmax=lonmax-spatial_reso}
    ## treatment
    cellsdimlat <-  ceiling(abs(latmax-latmin)/spatial_reso)
    cellsdimlon <-  ceiling(abs((lonmax-lonmin)/spatial_reso))
    ## create a centered grid 
    if (centred==T){
      smallest_lon <- ceiling(lonmin/spatial_reso)*spatial_reso + spatial_reso/2
      smallest_lat <- ceiling(latmin/spatial_reso)*spatial_reso + spatial_reso/2
    } else {
      smallest_lon <- lonmin + spatial_reso/2
      smallest_lat <- latmin + spatial_reso/2
    }
    grid = GridTopology(cellcentre.offset=c(smallest_lon,smallest_lat), cellsize=c(spatial_reso,spatial_reso), cells.dim=c(cellsdimlon,cellsdimlat))
    sp_zone <- as.SpatialPolygons.GridTopology(grid, proj4string = CRS(crs))
    
    return(sp_zone)
  }
