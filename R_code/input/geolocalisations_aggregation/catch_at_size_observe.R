######## Specific parameters for catch at size from observe database (ob7, IRD)
# Author : Chloe Dalleau, Geomatic engineer
# Supervisors : Paul Taconet (IRD), Julien barde (IRD)
# Date : 15/02/2018
#
# Functions list:
# 1. bdd_parameters : parameters for observe database connection and query to extract catch at size
# 2. aggregation_parameters : parameters for the function geolocalisation_aggregation


bdd_parameters <- function(firstDate,finalDate,sql_limit){
  #' @name bdd_parameters
  #' @title parameters for observe database connection and query to extract catch at size
  #' @description Parameters for observe database connection and query to extract catch at size. Warning : the database connections are confidential.
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
  
  ######## Size measure type
  ## Select size measure type wanted
  ## WARNING : a fish can be measured several times with different measures types
  ## list of size measures types possible
  # "L1" : depend of species, see observator manual
  # "FL" : "Fork Length"
  # "CFL" : "Curved Fork Length"
  # "PAL" : "Pectoral-Anal Length"
  # "TL" : "Total Length"
  # "SL" : "Standard Length"
  # "LJFL" : "Lower Jaw-Fork Length"
  # "CLJFL" : "Curved Lower Jaw-Fork Length"
  # "IDL" : "Interdorsal Length"
  # "EFL" : "Eye-Fork Length"
  # "DW" : "Disk Width"
  # "DL" : "Disk Length"
  # "TW" : "Total Weight"
  # "CTL" : "Curved Total Length"
  # "DML" : "Dorsal Mantle Length"
  # "CDML" : "Curved Dorsal Mantle Length"
  # "PFL" : "Pectoral-Fork Length"
  # "PD1" : "Predorsal Length"
  # "SCL" : "Straight Carapace Length"
  # "CCL" : "Curved Carapace Length"
  # "SCW" : "Straight Carapace Width"
  # "CCW" : "Curved Carapace Width"
  ## for size measure type comparable to total length put 'c("L1","TL", "SL", "LJFL", "CLJFL", "EFL", "DL", "CTL", "SCL", "CCL")'
  size_measure_type = c("L1","TL", "SL", "LJFL", "CLJFL", "EFL", "DL", "CTL", "SCL", "CCL", "FL")
  
  ######## SQL query to extract fishing catch at size
  query <- paste0("      WITH

                  -------------------------------------------------------------------------
                  -- Préparation des données de calées
                  -------------------------------------------------------------------------
                  -- extraction des données de longline en utilisant les schémas :
                  -- * observe_longline
                  -- * observe_common
                  union_catch_LL AS (
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
                  -- lors de marée commerciale auto-échantillonée ( explication dans poids de capture)
                  -- alors l'information n'existe qu'en nombre de capture
                  WHEN (observe_longline.catch.totalweight IS NULL) OR (triptype.code = 'SR'::text)
                  THEN 'NO'::text
                  ELSE 'NOMT'::text 
                  END AS catchunit,
                  observe_longline.sizemeasure.size AS sizemin,
                  observe_longline.sizemeasuretype.code AS sizetype,
                  observe_longline.catch.count AS catch
                  FROM 
                  observe_longline.catch
                  -- sélecton des captures avec une donnée de taille
                  INNER JOIN observe_longline.sizemeasure ON catch.topiaid=sizemeasure.catch
                  INNER JOIN observe_longline.sizemeasuretype ON sizemeasuretype.topiaid=sizemeasure.sizemeasuretype
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
                  -- condition théoriquement inutile 
                  vesselactivity.code='FO'::text AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_longline.sizemeasure.size IS NOT NULL
                  -- limit 10
                  )
                  UNION 
                  -- poids des captures
                  -- les poids associé au marée commerciale auto échantillonnée ne sont pas compté 
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
                  observe_longline.sizemeasure.size AS sizemin,
                  observe_longline.sizemeasuretype.code AS sizetype,
                  observe_longline.catch.totalweight AS catch
                  FROM 
                  observe_longline.catch
                  -- sélecton des captures avec une donnée de taille
                  INNER JOIN observe_longline.sizemeasure ON catch.topiaid=sizemeasure.catch
                  INNER JOIN observe_longline.sizemeasuretype ON sizemeasuretype.topiaid=sizemeasure.sizemeasuretype
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
                  vesselactivity.code='FO'::text AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_longline.sizemeasure.size IS NOT NULL
                  -- limit 10
                  )
                  ), longline AS (
                  SELECT
                  observe_longline.set.topiaid AS n_set,
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
                  WHEN observe_common.sex.code='1'::text THEN 'M'::text 
                  WHEN observe_common.sex.code='2'::text THEN 'F'::text 
                  ELSE 'UNK'::text
                  END AS sex,
                  1::numeric AS sizeinterval,
                  union_catch_LL.sizemin,
                  union_catch_LL.sizetype,
                  CASE 
                  WHEN observe_longline.catchfate.code ='SOLD'::text THEN 'SOLD'::text -- débarqué
                  WHEN observe_longline.catchfate.code ='UNK'::text THEN 'UNK'::text -- inconnue
                  WHEN observe_longline.catchfate.code ='ESC'::text THEN 'ESC'::text -- échappé
                  WHEN observe_longline.catchfate.code ='DISC'::text THEN 'D'::text -- rejté
                  WHEN observe_longline.catchfate.code ='KEPT'::text THEN 'USE.PRE.L'::text -- utilisé avant débarquement
                  END AS catchtype,
                  union_catch_LL.catchunit,
                  union_catch_LL.catch,
                  -- calcul du centroïde de la callee
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
                  LEFT JOIN observe_common.sex ON sex.topiaid::text = catch.sex::text
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
                  (
                  SELECT 
                  observe_seine.set.topiaid AS id_set,
                  observe_common.program.topiaid AS id_program,
                  observe_common.species.faocode AS species,  
                  CASE
                  WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                  WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                  WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                  END AS schooltype,  		  
                  observe_seine.targetlength.length AS sizemin, 
                  observe_common.species.lengthmeasuretype AS sizetype,
                  CASE 
                  WHEN observe_seine.targetsample.discarded=TRUE::boolean THEN 'D'::text
                  WHEN observe_seine.targetsample.discarded=FALSE::boolean THEN 'L'::text
                  END AS catchtype,
                  -- pour le nombre de capture il existe 2 types d'unité:
                  -- NO : l'information de capture existe uniquement en nombre
                  -- NOMT : l'information de capture existe également en poids	
                  CASE 
                  -- si l'information de poids est NULL 
                  -- alors l'information n'existe qu'en nombre
                  WHEN observe_seine.targetlength.weight IS NULL THEN 'NO'::text
                  ELSE 'NOMT'::text
                  END AS catchunit,
                  observe_seine.targetlength.count AS catch,
                  'UNK'::text AS sex
                  FROM 
                  observe_seine.targetlength 
                  INNER JOIN observe_seine.targetsample ON targetlength.targetsample = targetsample.topiaid
                  LEFT JOIN observe_common.species ON targetlength.species = species.topiaid
                  INNER JOIN observe_seine.set ON targetsample.set = set.topiaid
                  LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                  LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                  LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                  LEFT JOIN observe_common.program ON trip.program=program.topiaid		  
                  WHERE 
                  (observe_seine.targetlength.count IS NOT NULL) AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_seine.targetlength.length IS NOT NULL
                  -- limit 5
                  )
                  UNION
                  -- poids des captures
                  (
                  SELECT 
                  observe_seine.set.topiaid AS id_set,
                  observe_common.program.topiaid AS id_program,
                  observe_common.species.faocode AS species,  
                  CASE
                  WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                  WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                  WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                  END AS schooltype, 
                  observe_seine.targetlength.length AS sizemin, 
                  observe_common.species.lengthmeasuretype AS sizetype,
                  CASE 
                  WHEN observe_seine.targetsample.discarded=TRUE::boolean THEN 'D'::text
                  WHEN observe_seine.targetsample.discarded=FALSE::boolean THEN 'L'::text
                  END AS catchtype,
                  -- pour le poids de capture (tonne) il existe 2 types d'unité:
                  -- MT : l'information de capture existe uniquement en poids
                  -- MTNO : l'information de capture existe également en nombre			
                  CASE 
                  -- si l'information de nombre est NULL alors l'information n'existe qu'en poids
                  WHEN observe_seine.targetlength.count IS NULL THEN 'MT'::text
                  ELSE 'MTNO'::text
                  END AS catchunit,
                  observe_seine.targetlength.weight AS catch,
                  'UNK'::text AS sex
                  FROM 
                  observe_seine.targetlength 
                  INNER JOIN observe_seine.targetsample ON targetlength.targetsample = targetsample.topiaid
                  LEFT JOIN observe_common.species ON targetlength.species = species.topiaid
                  INNER JOIN observe_seine.set ON targetsample.set = set.topiaid
                  LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                  LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                  LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                  LEFT JOIN observe_common.program ON trip.program=program.topiaid
                  WHERE 
                  (observe_seine.targetlength.weight IS NOT NULL) AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_seine.targetlength.length IS NOT NULL
                  -- limit 5
                  )
                  UNION
                  
                  -- nontargetcatch
                  -- nombre de capture 
                  (
                  SELECT 
                  observe_seine.set.topiaid AS id_set,
                  observe_common.program.topiaid AS id_program,
                  observe_common.species.faocode AS species, 
                  CASE
                  WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                  WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                  WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                  END AS schooltype, 
                  observe_seine.nontargetlength.length AS sizemin, 
                  observe_common.species.lengthmeasuretype AS sizetype,
                  'UNK'::text AS catchtype,
                  -- pour le nombre de capture il existe 2 types d'unité:
                  -- NO : l'information de capture existe uniquement en nombre
                  -- NOMT : l'information de capture existe également en poids			
                  CASE 
                  -- si l'information de poids est NULL 
                  -- alors l'information n'existe qu'en nombre
                  WHEN observe_seine.nontargetlength.weight IS NULL THEN 'NO'::text
                  ELSE 'NOMT'::text
                  END AS catchunit,
                  observe_seine.nontargetlength.count AS catch, 
                  CASE 
                  WHEN observe_common.sex.code='1'::text THEN 'M'::text 
                  WHEN observe_common.sex.code='2'::text THEN 'F'::text 
                  ELSE 'UNK'::text
                  END AS sex 
                  FROM 
                  observe_seine.nontargetlength
                  INNER JOIN observe_seine.nontargetsample ON nontargetlength.nontargetsample = nontargetsample.topiaid
                  INNER JOIN observe_seine.set ON nontargetsample.set = set.topiaid 
                  LEFT JOIN observe_common.species ON nontargetlength.species = species.topiaid
                  LEFT JOIN observe_common.sex ON nontargetlength.sex = sex.topiaid
                  LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                  LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                  LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                  LEFT JOIN observe_common.program ON trip.program=program.topiaid	
                  WHERE  
                  (observe_seine.nontargetlength.count IS NOT NULL) AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_seine.nontargetlength.length IS NOT NULL
                  -- limit 5
                  )
                  UNION 
                  -- poids des captures
                  (
                  SELECT 
                  observe_seine.set.topiaid AS id_set,
                  observe_common.program.topiaid AS id_program,
                  observe_common.species.faocode AS species, 
                  CASE
                  WHEN observe_seine.set.schooltype = '2' THEN 'FS'::text -- banc libre, free school
                  WHEN observe_seine.set.schooltype = '1' THEN 'LS'::text -- banc sous object
                  WHEN observe_seine.set.schooltype = '3' THEN 'UNK'::text -- indéterminé, unknown
                  END AS schooltype,   
                  observe_seine.nontargetlength.length AS sizemin,  
                  observe_common.species.lengthmeasuretype AS sizetype,
                  'UNK'::text AS catchtype,
                  -- pour le poids de capture (tonne) il existe 2 types d'unité:
                  -- MT : l'information de capture existe uniquement en poids
                  -- MTNO : l'information de capture existe également en nombre			
                  CASE 
                  -- si l'information de nombre est NULL alors l'information n'existe qu'en poids
                  WHEN observe_seine.nontargetlength.count IS NULL THEN 'MT'::text 
                  ELSE 'MTNO'::text
                  END AS catchunit,
                  observe_seine.nontargetlength.weight AS catch, 
                  CASE 
                  WHEN observe_common.sex.code='1'::text THEN 'M'::text 
                  WHEN observe_common.sex.code='2'::text THEN 'F'::text 
                  ELSE 'UNK'::text
                  END AS sex 
                  FROM 
                  observe_seine.nontargetlength
                  INNER JOIN observe_seine.nontargetsample ON nontargetlength.nontargetsample = nontargetsample.topiaid
                  INNER JOIN observe_seine.set ON nontargetsample.set = set.topiaid 
                  LEFT JOIN observe_common.species ON nontargetlength.species = species.topiaid
                  LEFT JOIN observe_common.sex ON nontargetlength.sex = sex.topiaid
                  LEFT JOIN observe_seine.activity ON set.topiaid = activity.set
                  LEFT JOIN observe_seine.route ON route.topiaid=activity.route
                  LEFT JOIN observe_seine.trip ON route.trip=trip.topiaid
                  LEFT JOIN observe_common.program ON trip.program=program.topiaid
                  WHERE 
                  (observe_seine.nontargetlength.weight IS NOT NULL) AND
                  -- on ne sélectionne pas les captures qui n'ont pas été mesurées
                  observe_seine.nontargetlength.length IS NOT NULL
                  -- limit 5
                  )
                  
                  ), seine AS (
                  -- ensemble des données pour la senne
                  SELECT
                  CASE 
                  WHEN observe_common.ocean.code='1'::text THEN 'ATL'
                  WHEN observe_common.ocean.code='2'::text THEN 'IND'
                  WHEN observe_common.ocean.code='3'::text THEN 'PAC'
                  END AS ocean,
                  observe_common.country.iso3code AS flag,
                  'PS'::text AS gear,
                  observe_seine.route.date::date AS d_set,
                  observe_common.vessel.keelcode AS c_bat,
                  observe_common.program.label1 AS program,
                  union_catch_PS.schooltype,
                  union_catch_PS.species,
                  union_catch_PS.sex,
                  1::numeric AS sizeinterval,
                  union_catch_PS.sizemin,
                  CASE 
                  WHEN union_catch_PS.sizetype='LT'::text THEN 'TL'::text -- total length
                  WHEN union_catch_PS.sizetype='LF'::text THEN 'FL'::text -- fork length
                  WHEN union_catch_PS.sizetype='LC'::text THEN 'SCL'::text -- Straight Carapace Length
                  ELSE union_catch_PS.sizetype
                  END sizetype,
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
              (
                  SELECT 
                      longline.ocean,
                      longline.flag,
                      longline.gear,
                      longline.c_bat,
                      longline.d_set,
                      longline.program,
                      longline.schooltype,
                      longline.species,
                      longline.sex,
                      longline.sizeinterval,
                      longline.sizemin,
                      longline.sizetype,
                      longline.catchtype,
                      longline.catchunit,
                      longline.catch,
                      ST_X(longline.the_geom) AS lon,
                      ST_Y(longline.the_geom) AS lat
                  FROM 
                  longline
                  WHERE
                  -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
                  (longline.d_set BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date) AND
                  (longline.sizetype IN ('",paste(size_measure_type, collapse="', '"),"'))
                  ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)},"
              )
              UNION
              (
                  SELECT 
                      seine.ocean,
                      seine.flag,
                      seine.gear,
                      seine.c_bat,
                      seine.d_set,
                      seine.program,
                      seine.schooltype,
                      seine.species,
                      seine.sex,
                      seine.sizeinterval,
                      seine.sizemin,
                      seine.sizetype,
                      seine.catchtype,
                      seine.catchunit,
                      seine.catch,
                      ST_X(seine.the_geom) AS lon,
                      ST_Y(seine.the_geom) AS lat
                  FROM 
                  seine
                  WHERE
                  -- filtrage sur les activités comprisent entre la première et dernière date du calendrier
                  (seine.d_set BETWEEN '",firstDate,"'::date AND '",finalDate,"'::date) AND
                  (seine.sizetype IN ('",paste(size_measure_type, collapse="', '"),"'))
              
              ",if (!is.null(sql_limit)){paste0("limit ", sql_limit/2)},"
              )"
        )
  
  ######## colnames dataset
  ### Renamed column
  # - ocean : ocean where is located the activities. examples : "ATL" for Atlantic, "IND" for Indian
  # - flag : vessel flag. example : "FRA"
  # - gear : vessel gear. example : "PS"
  # - c_bat :  vessel keel code. example : "26"
  # - time : date of fishing data. example: "1990-10-27"
  # - program : trip program. example : "DCF IRD"
  # - schooltype : type of scool. example : "IND" (unknown), "BL" (free school), "BO" (school below an object)
  # - species : ID of species type (three letters). example : "SKJ"
  # - sex : fish sex. example : "IND" for unknown, "M" for male, "F" for female
  # - size_step : size step. fish size = size_min + size_step. example : "0", "1"
  # - size_min : minimal of size range. example : "10", "20"
  # - size_type : size measure type. example : "total length" "eye-fork length"
  # - catchtype : type of catch. example : "L" for captured, "D" for rejected
  # - catchunit : catch unit. example : "NO" for number of fish
  # - v_catch : quantity of catches. example : "1.02"
  # - lat, lon: latitude and longitude of fishing data
  colnames_dataset <- c("ocean", "flag",  "gear", "c_bat", "time", "program","schooltype", "species", "sex", "size_step", "size_min","size_type","catchtype", "catchunit", "v_catch", "lon","lat")

  ######## create output
  connection_parameters <- list(dbname,host,port,user,password,query,colnames_dataset)
  names(connection_parameters) <- c("dbname","host","port","user","password","query","colnames_dataset")
  
  return(connection_parameters)
}

aggregation_parameters <- function(program=F){
  #' @name aggregation_parameters
  #' @title parameters for aggregation of catch at size from observe database
  #' @description Parameters for aggregation of catch at size from observe database. These parameters are used in function : https://raw.githubusercontent.com/cdalleau/geolocations_and_trajectories_aggregation/master/R_code/functions/geolocalisation_aggregation.R
  #' 
  #' @param program Put TRUE to add fishing program in output dimensions, type = boolean;
  #' 
  #' @return Return a list with aggregation parameters for function geolocalisation_aggregation.R . This list contains : list of dimension to aggregate data, name of the fact (like: "catch", "effort", "catch_at_size", "fad"), database name, the variable to sum,the dimension to subset correctly the data (memory error)
  #' 
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' 
  #'
  #' @usage
  #'     aggregation_parameters <- aggregation_parameters(program=T)

  list_dimensions_output = c("ocean", "flag", "gear",if(program==T){c("program")}, "schooltype","species","sex","size_step","size_min","size_type","catchtype","catchunit")
  fact_name = "catch_at_size"
  bdd_name = "observe"
  var_aggregated_value = "v_catch"
  sub_dataset = "c_bat"
  
  parameters <- list(list_dimensions_output,var_aggregated_value, sub_dataset,fact_name,bdd_name)
  names(parameters) <- c("list_dimensions_output","var_aggregated_value","sub_dataset","fact_name","bdd_name")
  
  return(parameters)
}



