######## Specific parameters for catch at size from t3p database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for t3p database connection and query to extract catch at size
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for t3p database connection and query to extract catch at size
  #' @description Parameters for t3p database connection and query to extract catch at size. Warning : the database connections are confidential.
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

  ######## connecttion parameter
  dbname = "t3_prod"
  host = "aldabra2"
  port = 5432
  user = "***" 
  password = "***"
  
  
  ######## SQL query to extract fishing catch at size
  query <- paste0("     SELECT
                  CASE
                  WHEN ocean.code=1 THEN 'ATL'::text -- ocean atlantique
                  WHEN ocean.code=2 THEN 'IND'::text -- ocean indien
                  WHEN ocean.code=3 THEN 'PAC'::text -- ocean pacifique
                  ELSE 'UNK'::text
                  END AS ocean,
                  country.codeiso3 as flag,
                  CASE
                  WHEN vesselsimpletype.code=1 THEN 'PS'::text
                  WHEN vesselsimpletype.code=2 THEN 'BB'::text
                  WHEN vesselsimpletype.code=3 THEN 'UNK'::text --correspond à 'autre' de T3+
                  WHEN vesselsimpletype.code=4 THEN 'LL'::text
                  WHEN vesselsimpletype.code=5 THEN 'MIS'::text
                  WHEN vesselsimpletype.code=6 THEN 'NK'::text
                  END AS gear,
                  vessel.keelcode AS c_bat,
                  activity.date::DATE AS d_act,
                  activity.number::numeric AS n_act,
                  CASE
                  WHEN schooltype.libelle4 = 'BL' THEN 'FS'::text -- banc libre, free school
                  WHEN schooltype.libelle4 = 'BO' THEN 'LS'::text -- banc sous object
                  WHEN schooltype.libelle4 = 'IND' THEN 'UNK'::text -- indéterminé, unknown
                  END AS schooltype, 
                  species.code3l AS species,
                  'IND'::text AS sex,
                  -- catchtype : rejeté ou capturé
                  CASE
                  -- ATTENTION les espèces considérées en tant que rejet on un code espèce compris dans l'interval [8, 800:899]
              WHEN species.code = 8::numeric THEN 'D'::text
              WHEN (species.code >= 800 AND species.code <= 899) THEN 'D'::text
              ELSE 'L'::text
              END AS catchtype,
              'NO'::text AS catchunit, -- NO: nombre d'individu
                  1::numeric AS sizeinterval,
                  samplesetspeciesfrequency.lflengthclass AS sizemin,
                  samplesetspeciesfrequency.number::numeric AS catch,
                  activity.longitude AS v_lo_act,
                  activity.latitude AS v_la_act
                  FROM
                  public.activity
                  INNER JOIN public.samplewell ON samplewell.activity=activity.topiaid
                  INNER JOIN public.samplesetspeciesfrequency ON samplesetspeciesfrequency.samplewell=samplewell.topiaid
                  INNER JOIN public.species ON samplesetspeciesfrequency.species=species.topiaid
                  INNER JOIN public.schooltype ON (activity.schooltype=schooltype.topiaid)
                  INNER JOIN public.trip ON (activity.trip=trip.topiaid)
                  INNER JOIN public.ocean ON (activity.ocean=ocean.topiaid)
                  INNER JOIN public.vessel ON (trip.vessel = vessel.topiaid)
                  INNER JOIN public.country ON (vessel.flagcountry=country.topiaid)
                  INNER JOIN public.vesseltype ON (vessel.vesseltype = vesseltype.topiaid)
                  INNER JOIN public.vesselsimpletype ON (vesseltype.vesselsimpletype = vesselsimpletype.topiaid)
                  WHERE
              -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
              (activity.date BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
              ORDER BY
              ocean, flag, gear, c_bat, n_act, schooltype, species, sizemin
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
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - species : ID of species type (three letters). example : "SKJ"
  # - sex : fish sex. example : "IND" for unknown, "M" for male, "F" for female
  # - catchtype : type of catch. example : "L" for captured, "D" for rejected
  # - catchunit : catch unit. example : "NO" for number of fish
  # - size_step : size step. fish size = size_min + size_step. example : "0", "1"
  # - size_min : minimal of size range. example : "10", "20"
  # - v_catch : quantity of catches. example : "1.02"
  # - lat, lon : latitude and longitude of fishing data
  colnames_dataset <- c("ocean", "flag",  "gear", "c_bat", "time", "n_act", "schooltype", "species", "sex","catchtype", "catchunit", "size_step", "size_min", "v_catch", "lon","lat")

  ######## create function output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of catch at size from t3p database
  #' @description Parameters for aggregation of catch at size from t3p database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
  #'
  #' 
  #' @return Return a list with aggregation parameters for function geolocalisation_aggregation.R . This list contains : list of dimension to aggregate data, name of the fact (like: "catch", "effort", "catch_at_size", "fad"), database name, the variable to sum,the dimension to subset correctly the data (memory error)
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     aggregation_parameters <- aggregation_parameters()

  list_dimensions_output = c("ocean", "flag", "gear","species", "schooltype","sex","size_step","size_min","catchtype","catchunit")
  fact_name = "catch_at_size"
  bdd_name = "t3p"
  var_aggregated_value = "v_catch"
  sub_dataset = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","bdd_name")
  
  return(parameters)
}



