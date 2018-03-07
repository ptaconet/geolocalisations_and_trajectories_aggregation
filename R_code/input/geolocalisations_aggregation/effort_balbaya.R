######## Specific parameters for fishing effort from balbaya database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for balbaya database connection and query to extract fishing effort
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for balbaya database connection and query to extract fishing effort
  #' @description Parameters for balbaya database connection and query to extract fishing effort. Warning : the database connections are confidential.
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
  dbname = "balbaya"
  host = "aldabra2"
  port = 5432
  user = "***" 
  password = "***"
  
  
  
  ######## SQL query to extract fishing effort
  query<- paste("
                WITH
                -------------------------------------------------------------------------
                -- Selection des activites avec les dimensions souhaitees
                -------------------------------------------------------------------------
                union_effort AS (
                -- fusion des differents types d'effort dans la colonne effort
                -- la colonne unite d'effort permet de differencier les types d'effort
                      (
                      SELECT 
                      -- temps en mer
                      activite.c_bat,
                      activite.d_act, 
                      activite.n_act,
                      'ALL'::text AS settype,
                      'HOURS'::text AS effortunit,
                      activite.v_tmer AS effort
                      FROM 
                      public.activite
                      WHERE
                      -- filtrage sur les activites de peche
                      -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne une epoque
                      (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND
                      activite.v_tmer IS NOT NULL
                      -- limit 10
                      )
                UNION
                      (
                      SELECT 
                      -- temps de recherche 
                      activite.c_bat,
                      activite.d_act, 
                      activite.n_act,
                      'ALL'::text AS settype,
                      'FHOURS'::text AS effortunit,
                      activite.v_tpec AS effort
                      FROM 
                      public.activite
                      WHERE
                      -- filtrage sur les activites de peche
                      -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne une epoque
                      (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND
                      activite.v_tpec IS NOT NULL
                      -- limit 10
                      )
                UNION
                      (
                      SELECT 
                      -- duree de calee
                      activite.c_bat,
                      activite.d_act, 
                      activite.n_act,
                      CASE 
                      WHEN activite.v_nb_calee_pos >0 THEN 'POS'
                      WHEN activite.v_nb_calee_neg >0 THEN 'NEG'
                      ELSE 'ALL'
                      END AS settype,
                      'DUR.SETS'::text AS effortunit,
                      activite.v_dur_cal AS effort
                      FROM 
                      public.activite	
                      WHERE
                      -- filtrage sur les activites de peche
                      -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne une epoque
                      (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND
                      activite.v_dur_cal !=0 OR NOT NULL
                      -- limit 10
                      )
                UNION 
                      (
                      SELECT 
                      -- nombre de set positif
                      activite.c_bat,
                      activite.d_act, 
                      activite.n_act,
                      'POS'::text AS settype,
                      'SETS'::text AS effortunit,
                      activite.v_nb_calee_pos AS effort
                      FROM 
                      public.activite	
                      WHERE
                      -- filtrage sur les activites de peche
                      -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne une epoque
                      (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND
                      activite.v_nb_calee_pos!=0 OR NOT NULL	
                      -- limit 10
                )
                UNION 
                      (
                      SELECT 
                      -- nombre de calee NULL (negatif)
                      activite.c_bat,
                      activite.d_act, 
                      activite.n_act,
                      'NEG'::text AS settype,
                      'SETS'::text AS effortunit,
                      activite.v_nb_calee_neg AS effort
                      FROM 
                      public.activite	
                      
                      WHERE
                      -- filtrage sur les activites de peche
                      -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne a une epoque
                      (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) ) AND
                      activite.v_nb_calee_neg !=0 OR NOT NULL	
                      -- limit 10
                      )	
                )
                SELECT  
                CASE
                WHEN activite.c_ocea=1 THEN 'ATL'::text -- ocean atlantique
                WHEN activite.c_ocea=2 THEN 'IND'::text -- ocean indien
                WHEN activite.c_ocea=3 THEN 'PAC'::text -- ocean pacifique
                ELSE 'UNK'::text
                END AS ocean,
                pavillon.c_pays_fao AS flag,  
                engin.c_engin_4l AS gear, 
                union_effort.c_bat,
                union_effort.d_act, 
                union_effort.n_act,
                CASE
                WHEN type_banc.l4c_tban = 'BL' THEN 'FS'::text -- banc libre, free school
                WHEN type_banc.l4c_tban = 'BO' THEN 'LS'::text -- banc sous object
                WHEN (type_banc.l4c_tban = 'IND' OR NULL) THEN 'UNK'::text -- indetermine, unknown
                END AS schooltype,
                union_effort.settype,
                union_effort.effortunit,
                union_effort.effort,
                activite.v_la_act AS lat_data, 
                activite.v_lo_act AS lon_data
                FROM 
                union_effort
                LEFT JOIN public.activite ON (union_effort.c_bat=activite.c_bat AND 
                union_effort.d_act=activite.d_act AND
                union_effort.n_act=activite.n_act)
                LEFT JOIN public.type_banc ON activite.c_tban = type_banc.c_tban
                LEFT JOIN public.bateau ON activite.c_bat = bateau.c_bat
                LEFT JOIN public.pavillon ON bateau.c_pav_b = pavillon.c_pav_b 
                LEFT JOIN public.type_bateau ON bateau.c_typ_b = type_bateau.c_typ_b 
                LEFT JOIN public.engin ON type_bateau.c_engin = engin.c_engin 
                WHERE
                -- filtrage sur les activites comprisent entre la premiere et derniere date du calendrier
                (activite.d_act BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date) AND
                -- filtrage sur les activites de peche
                -- les types d'operation NULL sont comptabilise car elles n'etaient pas renseigne a une epoque
                (public.activite.c_opera IS NULL OR public.activite.c_opera IN (0,1,2,14) )
                ORDER BY 
                ocean, flag, gear, c_bat, d_act, n_act,v_tmer, v_tpec, v_dur_cal,
                schooltype, lat_data, lon_data, the_geom   
                ",if (!is.null(sql_limit)){paste0("limit ", sql_limit)}
                
                , sep="")

  ######## colnames dataset
  ### Renamed column
  # - ocean : ocean where is located the activities. examples : "ATL" for Atlantic, "IND" for Indian
  # - flag : vessel flag. example : "FRA"
  # - gear : vessel gear. example : "PS"
  # - c_bat : optional. vessel keel code. example : "26"
  # - time : date of fishing data. example: "1990-10-27"
  # - n_act : set number. example: "1990-10-27"
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - settype : optional. type of set. example : "POS" (positive), "NEG" (negative/null), "ALL" (positive + negative/null)
  # - effortUnits : effort unit. example : "SETS" number of set, "DUR.SETS" for sets duration, "HOURS" sea duration, "FHOURS" fishing duration, "BOATS" number of boat
  # - v_effort :  effort. example : "1.02"
  # - lat, lon : latitude and longitude of fishing data
  colnames_dataset <- c( "ocean", "flag", "gear", "c_bat", "time", "n_act", "schooltype", "settype", "effortunit", "v_effort", "lat", "lon")
  
  ######## create output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of fishing effort from balbaya database
  #' @description Parameters for aggregation of fishing effort from balbaya database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
  #' 
  #' @param program Put TRUE to add fishing program in output dimensions, type = boolean;
  #' 
  #' @return Return a list with aggregation parameters for function geolocalisation_aggregation.R . This list contains : list of dimension to aggregate data, name of the fact (like: "catch", "effort", "catch_at_size", "fad"), database name, the variable to sum, the dimension to subset correctly the data (memory error)
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     aggregation_parameters <- aggregation_parameters(program=T)

  list_dimensions_output = c("ocean", "flag", "gear", "schooltype","settype","effortunit")
  fact_name = "effort"
  bdd_name = "balbaya"
  var_aggregated_value = "v_effort"
  sub_dataset = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","bdd_name")
  
  return(parameters)
  }


