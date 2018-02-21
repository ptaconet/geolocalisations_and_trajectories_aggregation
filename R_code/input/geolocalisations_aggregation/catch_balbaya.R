######## Specific parameters for catch from balbaya database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for balbaya database connection and query to extract catch
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for balbaya database connection and query to extract catch
  #' @description Parameters for balbaya database connection and query to extract catch. Warning : the database connections are confidential.
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
  dbname = "***"
  host = "***"
  port = ***
  user = "***" 
  password = "***"
  
  
  ######## SQL query to extract catch
  query <- paste0("     SELECT  
                  CASE
                  WHEN activite.c_ocea=1 THEN 'ATL'::text -- ocean atlantique
                  WHEN activite.c_ocea=2 THEN 'IND'::text -- ocean indien
                  WHEN activite.c_ocea=3 THEN 'PAC'::text -- ocean pacifique
                  ELSE 'UNK'::text
                  END AS ocean,
                  pavillon.c_pays_fao AS flag,  
                  engin.c_engin_4l AS gear, 
                  activite.c_bat,
                  activite.d_act, 
                  activite.n_act, 
                  espece.c_esp_3l AS species,
                  CASE
                  WHEN type_banc.l4c_tban = 'BL' THEN 'FS'::text -- banc libre, free school
                  WHEN type_banc.l4c_tban = 'BO' THEN 'LS'::text -- banc sous object
                  WHEN type_banc.l4c_tban = 'IND' THEN 'UNK'::text -- indetermine, unknown
                  END AS schooltype, 
                  capture.c_cat_p, -- categorie de poids
                  CASE
                  -- ATTENTION les especes considerees en tant que rejet on un code espece compris dans l'interval [8, 800:899]
              WHEN espece.c_esp = 8::numeric THEN 'D'::text --Discard, rejete
              WHEN (espece.c_esp >= 800 AND espece.c_esp <= 899) THEN 'D'::text --Discard, rejete
              ELSE 'L'::text -- Landing
              END AS catchtype, 
              'MT'::text AS catchunit, -- unite de capture fixe :tonne
              ROUND(capture.v_poids_capt * activite.rf3,3) AS catch,-- rf3 : ratio de capture
              activite.v_la_act, 
              activite.v_lo_act

              FROM 
              public.activite
              INNER JOIN public.capture ON (activite.c_bat = capture.c_bat AND
              activite.d_act = capture.d_act AND
              activite.n_act = capture.n_act)
              LEFT JOIN public.bateau ON activite.c_bat = bateau.c_bat
              LEFT JOIN public.pavillon ON bateau.c_pav_b = pavillon.c_pav_b 
              LEFT JOIN public.type_bateau ON bateau.c_typ_b = type_bateau.c_typ_b 
              LEFT JOIN public.engin ON type_bateau.c_engin = engin.c_engin 
              INNER JOIN public.espece ON capture.c_esp = espece.c_esp
              LEFT JOIN public.type_banc ON activite.c_tban = type_banc.c_tban
              
              WHERE
              -- filtrage sur les activite de peche
              -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne a une epoque
              (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND 
              (public.activite.d_act BETWEEN '",firstDate,"' AND '",finalDate,"')
              
              ORDER BY 
              ocean, flag, gear, c_bat, d_act, n_act, 
              species, schooltype, v_la_act, v_lo_act, the_geom 
              ",if (!is.null(sql_limit)){paste0("limit ", sql_limit)}
             
        )
  
  ######## colnames dataset
  ### Renamed column
  # - ocean : ocean where is located the activities. examples : "ATL" for Atlantic, "IND" for Indian
  # - flag : vessel flag. example : "FRA"
  # - gear : vessel gear. example : "PS"
  # - c_bat : optional. vessel keel code. example : "26"
  # - time : date of fishing data. example: "1990-10-27"
  # - n_act : set number. example: "1990-10-27"
  # - species : ID of species type (three letters). example : "SKJ"
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - c_cat_p : weigth category
  # - catchtype : type of catch. example : "L" for captured, "D" for rejected
  # - catchunit : catch unit. example : "NO" for number of fish
  # - v_catch : quantity of catches. example : "1.02"
  # - lat, lon : latitude and longitude of fishing data
  colnames_dataset <- c("ocean", "flag", "gear", "c_bat", "time", "n_act", "species", "schooltype","c_cat_p","catchtype", "catchunit","v_catch","lat", "lon")
  
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
  #' @return Return a list with aggregation parameters for function geolocalisation_aggregation.R . This list contains : list of dimension to aggregate data, name of the fact (like: "catch", "effort", "catch_at_size", "fad"), database name, the variable to sum, the dimension object identifier (like : "c_bat")
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     aggregation_parameters <- aggregation_parameters()

  list_dimensions_output = c("ocean", "flag", "gear","species", "schooltype","catchtype","catchunit")
  fact_name = "catch"
  bdd_name = "balbaya"
  var_aggregated_value = "v_catch"
  object_identifier = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, object_identifier,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","object_identifier","fact_name","bdd_name")
  
  return(parameters)
}

