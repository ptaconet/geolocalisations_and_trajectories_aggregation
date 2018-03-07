######## Specific parameters for FAD from fads database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for fads database connection and query to extract catch
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for fads database connection and query to extract catch
  #' @description Parameters for fads database connection and query to extract catch. Warning : the database connections are confidential.
  #' 
  #' @param firstdate inferior limit date to extract data, format : YYYY-MM-DD, type = character;
  #' @param finaldate superior limit date to extract data, format : YYYY-MM-DD, type = character;
  #' @param sql_limit maximal number of raw in the output SQL query, type = integer;
  #' 
  #' @return Return a list with parameters connection for a PostgreSQL database, query and columne name of output data
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     bdd_parameters <- bdd_parameters(firstDate="2010-01-12",finalDate="2020-02-31",sql_limit=1000)

  ######## connection parameter
  dbname = "fads_20160813"
  host = "aldabra2"
  port = 5432
  user = "***" 
  password = "***"
  
  
  ######## SQL query to extract fad
  query <- paste0("                   

              SELECT
              positions_class.buoy_id AS fad_id,
              positions_class.section_num_flip2 AS section_id,
              positions_class.clean_pt_id AS pos_id,
              positions_class.pt_date::date AS date,
              ST_X(positions_class.pt_geom) AS lon_fad,
              ST_Y(positions_class.pt_geom) AS lat_fad,
              positions_class.class_flip2 AS fad_class

              FROM 
              fads_classified.positions_class		
              INNER JOIN fads_stats.segments_stats ON 
              (positions_class.buoy_id = segments_stats.buoy_id AND
              positions_class.clean_pt_id = segments_stats.start_clean_pt_id)
              
              WHERE 
              model_id=1 AND
              -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
              (positions_class.pt_date BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
              ",if (!is.null(sql_limit)){paste0("limit ", sql_limit)}
              
        )
  
  # colnames dataset
  ### Renamed column
  # - fad_class : fads classes. examples : "W" for at sea and "B" for on boat
  # - section : numbering of tracking section. exemple: 
  #           fad_class = B B W W W W B W W W 
  #           section =   1 1 2 2 2 2 3 4 4 4
  # - time_start : start of calendar. example : "1990-12-25"
  # - time_stop : end of calendar. example : "1991-01-08"
  # - area : geometric coordinates of polygons (spatial grid) 
  # - days : number of days. example : "2"
  # - fadunit : fads unit. example : "NO" for metric nomber
  # - fads : quantity of fads example : "1"
  colnames_dataset <- c( "fad_id", "section","pos_id", "time", "lon", "lat" ,"fad_class")
  
  ######## create output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of catch from balbaya database
  #' @description Parameters for aggregation of catch from balbaya database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
  #' 
  #' @param program Put TRUE to add fishing program in output dimensions, type = boolean;
  #' 
  #' @return Return a list with aggregation parameters for function geolocalisation_aggregation.R . This list contains : list of dimension to aggregate data, name of the fact (like: "catch", "effort", "catch_at_size", "fad"), database name, specify if the function calculate the number of days, the variable to sum, the dimension to subset correctly the data (memory error)
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     aggregation_parameters <- aggregation_parameters()
  list_dimensions_output = c("fad_class")
  fact_name = "fad"
  bdd_name = "fad"
  calculation_of_number_days = T
  var_aggregated_value = NULL
  sub_dataset = "fad_id"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,calculation_of_number_days,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","calculation_of_number_days","bdd_name")
  
  return(parameters)
}


