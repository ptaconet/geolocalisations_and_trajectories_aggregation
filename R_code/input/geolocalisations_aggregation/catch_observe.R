######## Specific parameters for catch from observe database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for observe database connection and query to extract catch
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for observe database connection and query to extract catch
  #' @description Parameters for observe database connection and query to extract catch. Warning : the database connections are confidential.
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
  
  
  ######## SQL query to extract catch
  query <- paste0("                   
                  WITH

                  -------------------------------------------------------------------------
                  -- Préparation des données de calées
                  -------------------------------------------------------------------------
                  -- extraction des données de longline en utilisant les schémas :
                  -- * observe_longline
                  -- * observe_common
                  union_catch_LL AS (
                      -- Données de capture en poid et en nombre d'individu
                      -- union_catch_LL contient les id_catch en double :
                      -- * une ligne pour le nombre de capture
                      -- * une ligne pour le poids des captures
                      
                      -- nombre de capture
                      (SELECT 
                      observe_longline.catch.topiaid::text AS id_catch,
                      observe_common.program.topiaid::text AS id_program,
                      -- pour le nombre de capture il existe en 2 types d'unité:
                      -- NO : l'information de capture existe uniquement en nombre de capture
                      -- NOMT : l'information de capture existe également en poids de capture
                      CASE 
                      -- si l'information de poids est NULL ou si l'information de poids a été obtenu
                      -- lors de marée commerciale auto-échantillonée 'SR' (le poids n'est pas retenu)
                      -- alors l'information n'existe qu'en nombre de capture
                      WHEN (observe_longline.catch.totalweight IS NULL) OR (triptype.code = 'SR'::text)
                      THEN 'NO'::text
                      ELSE 'NOMT'::text 
                      END AS catchunit,
                      observe_longline.catch.count AS catch
                      FROM 
                      observe_longline.catch
                      INNER JOIN observe_longline.set ON set.topiaid= catch.set
                      LEFT JOIN observe_longline.activity ON activity.set=set.topiaid
                      LEFT JOIN observe_longline.vesselactivity ON activity.vesselactivity=vesselactivity.topiaid
                      LEFT JOIN observe_longline.trip ON activity.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      LEFT JOIN observe_longline.triptype ON trip.triptype=triptype.topiaid
                      WHERE 
                      -- les captures NULL (en nombre) ne sont pas sélectionné
                      observe_longline.catch.count IS NOT NULL AND
                      -- seule les activité de type pêche sont sélectionnées
                      vesselactivity.code='FO'::text		
                      -- limit 10
                      )
                      UNION 
                      -- poids des captures
                      -- les poids associé aux marées commerciales auto échantillonnée ne sont pas comptées 
                      -- car l'estimation du poids n'est pas assez précis
                      (SELECT 
                      observe_longline.catch.topiaid::text AS id_catch,
                      observe_common.program.topiaid::text AS id_program,
                      -- pour le poids de capture (tonne) il existe 2 types d'unité:
                      -- MT : l'information de capture existe uniquement en poids
                      -- MTNO : l'information de capture existe également en nombre de capture			
                      CASE 
                      -- si l'information de nombre est NULL 
                      -- alors l'information n'existe qu'en poids
                      WHEN observe_longline.catch.count IS NULL THEN 'MT'::text
                      ELSE 'MTNO'::text
                      END AS cach_unit,
                      observe_longline.catch.totalweight AS catch
                      FROM 
                      observe_longline.catch
                      INNER JOIN observe_longline.set ON set.topiaid= catch.set
                      LEFT JOIN observe_longline.activity ON activity.set=set.topiaid
                      LEFT JOIN observe_longline.vesselactivity ON activity.vesselactivity=vesselactivity.topiaid
                      LEFT JOIN observe_longline.trip ON activity.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      LEFT JOIN observe_longline.triptype ON trip.triptype=triptype.topiaid
                      WHERE 
                      -- les captures NULL (en poids) ne sont pas sélectionnées
                      observe_longline.catch.totalweight IS NOT NULL AND 
                      -- marées commerciales auto échantillonnée non sélectionnées
                      triptype.code != 'SR' AND
                      -- seule les activité de type pêche sont sélectionné
                      -- condition théoriquement inutile 
                      vesselactivity.code='FO'::text
                      -- limit 10
                      )
                  
                  ), longline AS (
                      -- ensemble des données pour la longline
                      SELECT
                      
                      observe_longline.set.topiaid AS id,
                      CASE 
                      WHEN observe_common.ocean.code='1'::text THEN 'ATL'
                      WHEN observe_common.ocean.code='2'::text THEN 'IND'
                      WHEN observe_common.ocean.code='3'::text THEN 'PAC'
                      END AS ocean,
                      observe_common.country.iso3code AS flag,
                      'LL'::text AS gear,
                      observe_longline.set.haulingstarttimestamp::date AS d_set,
                      observe_common.vessel.keelcode AS c_bat,
                      observe_common.program.label1 AS program,
                      'ALL'::text AS schooltype,
                      observe_common.species.faocode AS species,
                      CASE 
                      WHEN observe_longline.catchfate.code ='SOLD'::text THEN 'SOLD'::text -- débarqué
                      WHEN observe_longline.catchfate.code ='UNK'::text THEN 'UNK'::text -- inconnue
                      WHEN observe_longline.catchfate.code ='ESC'::text THEN 'ESC'::text -- échappé
                      WHEN observe_longline.catchfate.code ='DISC'::text THEN 'D'::text -- rejté
                      WHEN observe_longline.catchfate.code ='KEPT'::text THEN 'USE.PRE.L'::text -- utilisé avant débarquement
                      END AS catchtype,
                      union_catch_LL.catchunit,
                      union_catch_LL.catch,
                      -- calcul du centroïde de la calée
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
                      observe_longline.catch
                      INNER JOIN union_catch_LL ON union_catch_LL.id_catch::text = catch.topiaid::text
                      INNER JOIN observe_common.species ON species.topiaid::text = catch.speciescatch::text
                      INNER JOIN observe_longline.set ON set.topiaid::text=catch.set::text
                      LEFT JOIN observe_longline.catchfate ON catchfate.topiaid::text=catch.catchfate::text
                      LEFT JOIN observe_longline.activity ON set.topiaid::text=activity.set::text
                      INNER JOIN observe_longline.trip ON trip.topiaid::text=activity.trip::text
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      INNER JOIN observe_common.vessel ON vessel.topiaid::text = trip.vessel::text
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
                      -- Données de capture en poid et en nombre d'individu
                      -- On dissocie les 'targetcatch' et 'nontargetcatch' car les données 
                      -- ne sont pas stockées de la même manière
                      -- union_catch_PS contient les id_catch en double :
                      -- * une ligne pour le nombre de capture
                      -- * une ligne pour le poids des captures
                      
                      -- targetcatch
                      -- nombre de capture
                      -- pas de données
                      -- poids des captures
                      (SELECT 
                      observe_seine.set.topiaid AS id_set,
                      observe_common.program.topiaid AS id_program,
                      observe_common.species.faocode AS species,  
                      CASE
                      WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                      WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                      WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                      END AS schooltype, 
                      CASE 
                      WHEN observe_seine.targetcatch.discarded=TRUE::boolean THEN 'D'::text
                      WHEN observe_seine.targetcatch.discarded=FALSE::boolean THEN 'L'::text
                      END AS catchtype,
                      'MT'::text AS catchunit, -- tonne
                      observe_seine.targetcatch.catchweight AS catch
                      FROM 
                      observe_seine.targetcatch
                      INNER JOIN observe_seine.set ON targetcatch.set = set.topiaid 
                      LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                      LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                      LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      LEFT JOIN observe_seine.weightcategory ON targetcatch.weightcategory = weightcategory.topiaid
                      LEFT JOIN observe_common.species ON weightcategory.species = species.topiaid		  
                      WHERE 
                      observe_seine.targetcatch.catchweight IS NOT NULL
                      -- limit 10
                      )
                      UNION
                      
                      -- nontargetcatch
                      -- nombre de capture 
                      
                      (SELECT 
                      observe_seine.set.topiaid AS id_set,
                      observe_common.program.topiaid AS id_program,
                      observe_common.species.faocode AS species, 
                      CASE
                      WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                      WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                      WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                      END AS schooltype, 
                      CASE
                      WHEN observe_seine.speciesfate.code ='1'::text THEN 'ESC'::text -- 'escape'
                      WHEN observe_seine.speciesfate.code ='2'::text THEN 'LIVE.ESC'::text -- live escape
                      WHEN observe_seine.speciesfate.code ='3'::text THEN 'DEAD.LOST'::text -- pre-cath losses
                      WHEN observe_seine.speciesfate.code ='4'::text THEN 'LIVE.DISC'::text -- live discard
                      WHEN observe_seine.speciesfate.code ='5'::text THEN 'DEAD.DISC'::text -- dead discard
                      WHEN observe_seine.speciesfate.code ='6'::text THEN 'RETAINED'::text --landing
                      WHEN observe_seine.speciesfate.code ='7'::text THEN 'PARTIAL.RETAINED'::text -- landing
                      WHEN observe_seine.speciesfate.code ='8'::text THEN 'USE.PRE.L'::text -- UTILIZATION AND LOSSES PRIOR TO LANDING 
                      WHEN observe_seine.speciesfate.code ='9'::text THEN 'UNK'::text -- unknown
                      WHEN observe_seine.speciesfate.code ='10'::text THEN 'FINS.DEAD.DISC'::text -- dead dscard
                      END AS catchtype,
                      -- pour le nombre de capture il existe 2 types d'unité:
                      -- NO : l'information de capture existe uniquement en nombre
                      -- NOMT : l'information de capture existe également en poids			
                      CASE 
                      -- si l'information de poids est NULL 
                      -- alors l'information n'existe qu'en nombre
                      WHEN observe_seine.nontargetcatch.catchweight IS NULL THEN 'NO'::text
                      ELSE 'NOMT'::text
                      END AS catchunit,
                      observe_seine.nontargetcatch.totalcount AS catch 
                      FROM 
                      observe_seine.nontargetcatch
                      INNER JOIN observe_seine.set ON nontargetcatch.set = set.topiaid 
                      LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                      LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                      LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      LEFT JOIN observe_common.species ON nontargetcatch.species = species.topiaid	
                      LEFT JOIN observe_seine.speciesfate ON nontargetcatch.speciesfate = speciesfate.topiaid 
                      WHERE 
                      observe_seine.nontargetcatch.totalcount IS NOT NULL
                      -- limit 10
                      )
                      UNION 
                      -- poids des captures
                      
                      (SELECT 
                      observe_seine.set.topiaid AS id_set,
                      observe_common.program.topiaid AS id_program,
                      observe_common.species.faocode AS species, 
                      CASE
                      WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                      WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                      WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                      END AS schooltype,  
                      CASE
                      WHEN observe_seine.speciesfate.code ='1'::text THEN 'ESC'::text -- 'escape'
                      WHEN observe_seine.speciesfate.code ='2'::text THEN 'LIVE.ESC'::text -- live escape
                      WHEN observe_seine.speciesfate.code ='3'::text THEN 'DEAD.LOST'::text -- pre-cath losses
                      WHEN observe_seine.speciesfate.code ='4'::text THEN 'LIVE.DISC'::text -- live discard
                      WHEN observe_seine.speciesfate.code ='5'::text THEN 'DEAD.DISC'::text -- dead discard
                      WHEN observe_seine.speciesfate.code ='6'::text THEN 'RETAINED'::text --landing
                      WHEN observe_seine.speciesfate.code ='7'::text THEN 'PARTIAL.RETAINED'::text -- landing
                      WHEN observe_seine.speciesfate.code ='8'::text THEN 'USE.PRE.L'::text -- UTILIZATION AND LOSSES PRIOR TO LANDING 
                      WHEN observe_seine.speciesfate.code ='9'::text THEN 'UNK'::text -- unknown
                      WHEN observe_seine.speciesfate.code ='10'::text THEN 'FINS.DEAD.DISC'::text -- dead dscard
                      END AS catchtype,
                      -- pour le poids de capture (tonne) il existe 2 types d'unité:
                      -- MT : l'information de capture existe uniquement en poids
                      -- MTNO : l'information de capture existe également en nombre			
                      CASE 
                      -- si l'information de nombre est NULL alors l'information n'existe qu'en poids
                      WHEN observe_seine.nontargetcatch.totalcount IS NULL THEN 'MT' 
                      ELSE 'MTNO'
                      END AS catchunit,
                      observe_seine.nontargetcatch.catchweight AS catch 
                      FROM 
                      observe_seine.nontargetcatch
                      INNER JOIN observe_seine.set ON nontargetcatch.set = set.topiaid 
                      LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                      LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                      LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid
                      LEFT JOIN observe_common.species ON nontargetcatch.species = species.topiaid	
                      LEFT JOIN observe_seine.speciesfate ON nontargetcatch.speciesfate = speciesfate.topiaid
                      WHERE 
                      observe_seine.nontargetcatch.catchweight IS NOT NULL
                      -- limit 10
                      )
                  
                  ), seine AS (
                      -- ensemble des données pour la senne
                      SELECT
                      union_catch_PS.id_set AS id,
                      CASE 
                      WHEN observe_common.ocean.code='1'::text THEN 'ATL' --atlantique
                      WHEN observe_common.ocean.code='2'::text THEN 'IND' --indien
                      WHEN observe_common.ocean.code='3'::text THEN 'PAC' -- pacifique
                      END AS ocean,
                      observe_common.country.iso3code AS flag,
                      'PS'::text AS gear,
                      observe_seine.route.date::date AS d_set,
                      observe_common.vessel.keelcode AS c_bat,
                      observe_common.program.label1 AS program,
                      union_catch_PS.schooltype,
                      union_catch_PS.species,
                      union_catch_PS.catchtype,
                      union_catch_PS.catchunit,
                      union_catch_PS.catch,
                      observe_seine.activity.the_geom
                      FROM 
                      union_catch_PS
                      JOIN observe_seine.set ON set.topiaid::text=union_catch_PS.id_set::text
                      LEFT JOIN observe_seine.activity ON set.topiaid::text=activity.set::text
                      LEFT JOIN observe_seine.route ON activity.route::text=route.topiaid::text
                      LEFT JOIN observe_seine.trip ON trip.topiaid::text=route.trip::text
                      LEFT JOIN observe_common.program ON trip.program=program.topiaid::text
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
              -- Esemble des données de calées pour la base Observe selon l'emprise spatiale les extremum du calendrier
              -------------------------------------------------------------------------
                  (SELECT 
                      longline.ocean,
                      longline.flag,
                      longline.gear,
                      longline.c_bat,
                      longline.d_set,
                      longline.program,
                      longline.schooltype,
                      longline.species,
                      longline.catchtype,
                      longline.catchunit,
                      longline.catch,
                      ST_X(longline.the_geom) AS lon,
                      ST_Y(longline.the_geom) AS lat
                  FROM 
                  longline
                  WHERE
                      -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
                      (longline.d_set BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
                  ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)},"
                )
              UNION
                  (SELECT 
                      seine.ocean,
                      seine.flag,
                      seine.gear,
                      seine.c_bat,
                      seine.d_set,
                      seine.program,
                      seine.schooltype,
                      seine.species,
                      seine.catchtype,
                      seine.catchunit,
                      seine.catch,
                      ST_X(seine.the_geom) AS lon,
                      ST_Y(seine.the_geom) AS lat
                  FROM 
                  seine
                  WHERE
              -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
              (seine.d_set BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date)
              ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)}
              ,")"
        )
  
  ######## colnames dataset
  ### Renamed column
  # - ocean : ocean where is located the activities. examples : "ATL" for Atlantic, "IND" for Indian
  # - flag : vessel flag. example : "FRA"
  # - gear : vessel gear. example : "PS"
  # - c_bat : optional. vessel keel code. example : "26"
  # - time : date of fishing data. example: "1990-10-27"
  # - program : trip program. example : "DCF IRD"
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - species : ID of species type (three letters). example : "SKJ"
  # - catchtype : type of catch. example : "L" for captured, "D" for rejected
  # - catchunit : catch unit. example : "NO" for number of fish
  # - v_catch : quantity of catches. example : "1.02"
  # - lat, lon : latitude and longitude of fishing data
  colnames_dataset <- c("ocean", "flag", "gear", "c_bat", "time", "program","schooltype","species","catchtype", "catchunit","v_catch","lon", "lat")
  
  ######## create output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(program=F){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of catch from observe database
  #' @description Parameters for aggregation of catch from observe database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
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

  list_dimensions_output = c("ocean", "flag", "gear",if(program==T){c("program")},"schooltype","species", "catchtype","catchunit")
  fact_name = "catch"
  bdd_name = "observe"
  var_aggregated_value = "v_catch"
  sub_dataset = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","bdd_name")
  
  return(parameters)
}


