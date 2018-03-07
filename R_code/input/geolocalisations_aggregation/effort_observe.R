######## Specific parameters for fishing effort from observe database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for observe database connection and query to extract fishing effort
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for observe database connection and query to extract fishing effort
  #' @description Parameters for observe database connection and query to extract fishing effort. Warning : the database connections are confidential.
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
  dbname = "observe"
  host = "aldabra2"
  port = 5432
  user = "***" 
  password = "***"
  
  
  
  ######## SQL query to extract fishing effort
  query<- paste("
                WITH
                -------------------------------------------------------------------------
                -- Selection des données dans Observe
                -------------------------------------------------------------------------
                -- extraction des données de palangrier en utilisant les schémas :
                -- * observe_longline
                -- * observe_common
                longline AS (
                    -- ensemble des données pour la longline
                    SELECT
                    observe_longline.set.topiaid AS id_set,
                    observe_longline.activity.topiaid AS id_act,
                    observe_common.program.label1 AS program,
                    CASE 
                    WHEN observe_common.ocean.code='1'::text THEN 'ATL'::text
                    WHEN observe_common.ocean.code='2'::text THEN 'IND'::text
                    WHEN observe_common.ocean.code='3'::text THEN 'PAC'::text
                    END AS ocean,
                    observe_common.country.iso3code::text AS flag,
                    'LL'::text AS gear,
                    observe_longline.set.haulingstarttimestamp::date AS date,
                    observe_common.vessel.keelcode::numeric AS c_bat,
                    'ALL'::text AS schooltype,
                    'ALL'::text AS settype,
                    'HOOKS'::text AS effortunit, --a verifier
                    observe_longline.set.totalhookscount::numeric AS effort,
                    --calcul du centroïde de la callee
                    ST_Centroid(
                    ST_ConvexHull(
                    ST_Collect(
                    ST_Collect(ST_SetSRID(ST_Point(observe_longline.set.settingstartlongitude, observe_longline.set.settingstartlatitude), 4326),
                    ST_SetSRID(ST_Point(observe_longline.set.settingendlongitude, observe_longline.set.settingendlatitude), 4326)),
                    ST_Collect(ST_SetSRID(ST_Point(observe_longline.set.haulingstartlongitude, observe_longline.set.haulingstartlatitude), 4326),
                    ST_SetSRID(ST_Point(observe_longline.set.haulingendlongitude, observe_longline.set.haulingendlatitude), 4326))
                    )
                    )
                    ) AS the_geom
                    
                    FROM 
                    observe_longline.set
                    LEFT JOIN observe_longline.activity ON set.topiaid::text=activity.set::text
                    INNER JOIN observe_longline.trip ON trip.topiaid::text=activity.trip::text
                    INNER JOIN observe_common.vessel ON vessel.topiaid::text = trip.vessel::text
                    LEFT JOIN observe_common.program ON trip.program=program.topiaid
                    INNER JOIN observe_common.country ON country.topiaid::text = vessel.flagcountry::text
                    INNER JOIN observe_common.ocean ON ocean.topiaid::text = trip.ocean::text
                    WHERE
                    -- selection des données des programmes : DCF IRD (longline et seine), DCF TAAF, Moratoire 2013+, OCUP
                    observe_common.program.topiaid IN 
                    ('fr.ird.observe.entities.referentiel.Program#1239832686139#0.1',
                    'fr.ird.observe.entities.referentiel.Program#1239832686262#0.31033946454061234',
                    'fr.ird.observe.entities.referentiel.Program#1308048349668#0.7314513252652438',
                    'fr.ird.observe.entities.referentiel.Program#1363095174385#0.011966550987014823',
                    'fr.ird.observe.entities.referentiel.Program#1373642516190#0.998459307142491')
               
                ),
                -- extraction des données de senne en utilisant les schémas :
                -- * observe_seine
                -- * observe_common
                union_catch_PS AS (
                      -- calée nulle
                      (SELECT 
                      observe_seine.activity.topiaid::text AS id_act,
                      NULL::text AS id_set,
                      observe_common.program.topiaid AS id_program,
                      observe_seine.route.date::date AS date,
                      'UNK'::text AS schooltype,
                      'NEG'::text AS settype,
                      'SETS'::text AS effortunit,
                      1::numeric AS effort
                      FROM 
                      observe_seine.activity
                      LEFT JOIN observe_seine.vesselactivity ON vesselactivity.topiaid=activity.vesselactivity 
                      LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                      LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      WHERE
                      observe_seine.activity.set IS NULL AND
                      observe_seine.vesselactivity.code='6'
                      -- limit 10
                      )
                      UNION
                      -- calée positive
                      (SELECT 
                      observe_seine.activity.topiaid::text AS id_act,
                      observe_seine.set.topiaid::text AS id_set,
                      observe_common.program.topiaid AS id_program,
                      observe_seine.set.endsettimestamp::date AS date,
                      CASE
                      WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                      WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                      WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                      END AS schooltype, 
                      'POS'::text AS settype,
                      'SETS'::text AS effortunit,
                      1::numeric AS effort
                      FROM 
                      observe_seine.set
                      LEFT JOIN observe_seine.activity ON activity.set=set.topiaid
                      LEFT JOIN observe_seine.vesselactivity ON vesselactivity.topiaid=activity.vesselactivity  
                      LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                      LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      WHERE
                      observe_seine.vesselactivity.code='6'	
                      -- limit 10
                      )
                ), seine AS (
                    SELECT
                    union_catch_PS.id_set,
                    union_catch_PS.id_act,
                    observe_common.program.label1 AS program,
                    CASE 
                    WHEN observe_common.ocean.code='1'::text THEN 'ATL'::text --atlantique
                    WHEN observe_common.ocean.code='2'::text THEN 'IND'::text --indien
                    WHEN observe_common.ocean.code='3'::text THEN 'PAC'::text -- pacifique
                    END AS ocean,
                    observe_common.country.iso3code::text AS flag,
                    'PS'::text AS gear,
                    union_catch_PS.date,
                    observe_common.vessel.keelcode AS c_bat,
                    union_catch_PS.schooltype,
                    union_catch_PS.settype,
                    union_catch_PS.effortunit,
                    union_catch_PS.effort,
                    observe_seine.activity.the_geom
                    FROM 
                    union_catch_PS
                    LEFT JOIN observe_seine.activity ON activity.topiaid::text=union_catch_PS.id_act::text
                    LEFT JOIN observe_seine.set ON set.topiaid::text=union_catch_PS.id_set::text
                    LEFT JOIN observe_seine.route ON activity.route::text=route.topiaid::text
                    LEFT JOIN observe_seine.trip ON trip.topiaid::text=route.trip::text
                    LEFT JOIN observe_common.program ON trip.program=program.topiaid
                    LEFT JOIN observe_common.vessel ON vessel.topiaid::text = trip.vessel::text
                    LEFT JOIN observe_common.country ON country.topiaid::text = vessel.flagcountry::text
                    LEFT JOIN observe_common.ocean ON ocean.topiaid::text = trip.ocean::text
                    WHERE
                    -- selection des données des programmes : DCF IRD (longline et seine), DCF TAAF, Moratoire 2013+, OCUP
                    observe_common.program.topiaid IN 
                    ('fr.ird.observe.entities.referentiel.Program#1239832686139#0.1',
                    'fr.ird.observe.entities.referentiel.Program#1239832686262#0.31033946454061234',
                    'fr.ird.observe.entities.referentiel.Program#1308048349668#0.7314513252652438',
                    'fr.ird.observe.entities.referentiel.Program#1363095174385#0.011966550987014823',
                    'fr.ird.observe.entities.referentiel.Program#1373642516190#0.998459307142491')
                
                )
                -------------------------------------------------------------------------
                -- Esemble des données de calées pour la base Observe
                -------------------------------------------------------------------------

                 ( 
                    SELECT 
                    longline.ocean,
                    longline.flag,
                    longline.program,
                    longline.gear,
                    longline.c_bat,
                    longline.date,
                    longline.schooltype,
                    longline.settype,
                    longline.effortunit,
                    longline.effort,
                    ST_X(longline.the_geom) AS lon,
                    ST_Y(longline.the_geom) AS lat
                    FROM 
                    longline
                    WHERE
                    (longline.date BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
                    ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)},"
                )
                UNION
                (
                    SELECT 
                    seine.ocean,
                    seine.flag,
                    seine.program,
                    seine.gear,
                    seine.c_bat,
                    seine.date,
                    seine.schooltype,
                    seine.settype,
                    seine.effortunit,
                    seine.effort,
                    ST_X(seine.the_geom) AS lon,
                    ST_Y(seine.the_geom) AS lat
                    FROM 
                    seine
                    WHERE
                    (seine.date BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
                    ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)}
                ,")"

                
                , sep="")

  ######## colnames dataset
  ### Renamed column
  # - ocean : ocean where is located the activities. examples : "ATL" for Atlantic, "IND" for Indian
  # - flag : vessel flag. example : "FRA"
  # - program : trip program. example : "DCF IRD"
  # - gear : vessel gear. example : "PS"
  # - c_bat : optional. vessel keel code. example : "26"
  # - time : date of fishing data. example: "1990-10-27"
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - settype : optional. type of set. example : "POS" (positive), "NEG" (negative/null), "ALL" (positive + negative/null)
  # - effortUnits : effort unit. example : "SETS" number of set, "DUR.SETS" for sets duration, "HOURS" sea duration, "FHOURS" fishing duration, "BOATS" number of boat
  # - v_effort :  effort. example : "1.02"
  # - lat, lon : latitude and longitude of fishing data
  colnames_dataset <- c("ocean", "flag", "program", "gear", "c_bat", "time", "schooltype", "settype", "effortunit", "v_effort",
                        "lon", "lat")
  
  ######## create output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(program=F){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of fishing effort from observe database
  #' @description Parameters for aggregation of fishing effort from observe database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
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

  list_dimensions_output = c("ocean", "flag", "gear",if(program==T){c("program")}, "schooltype","settype","effortunit")
  fact_name = "effort"
  bdd_name = "observe"
  var_aggregated_value = "v_effort"
  sub_dataset = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","bdd_name")
  
  return(parameters)
}

