####################### List of functions
# Author : Chloé Dalleau, Geomatic engineer (IRD)
# Supervisor : Julien Barde (IRD)
# Date : 16/02/2018 
# 
# ## Summary 
# 1. create_calendar : create a continious calendar in days, months or years

create_calendar <- function(firstdate,finaldate,temporal_reso,temporal_reso_unit){
  #' @name create_calendar
  #' @title Create continious calendar 
  #' @description Create a cotinious calendar according to input parameters : first and final date of the calendar, temporal resolution and temporal resolution unit (values : "day","month","year"). The algorithm can create a mi-month calendar : first period between 1st and 15th of a month, second period between 16th and the end of the month.
  #' 
  #' @param firstdate first date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param finaldate final date of the calendar, format : YYYY-MM-DD, type = character;
  #' @param temporal_reso temporal resolution of the calendar in day, month or year (see: temporal_reso_unit). Note: for 1/2 month put temporal_reso=1/2 and temporal_reso_unit="month" , type = integer;
  #' @param temporal_reso_unit temportal resolution unit od calendar, accepted value : "day" "month" "year", type = character;
  #'
  #' @return Return a dataframe with two columns : "time_start" (first date of the period) and "time_end" (end of the period).
  #'
  #' @author Chloé Dalleau, \email{chloe.dalleau@ird.fr}
  #' @keywords continious calendar, day, month, year
  #'
  #' @usage
  #'  calendar <- create_calendar(firstdate="2010-01-01",finaldate="2010-12-31",temporal_reso=5,temporal_reso_unit="day")
  
  ### Package 
  require(lubridate)
  
  ### Initialisation
  firstdate <- as_date(firstdate, tz = "UTC")
  finaldate <- as_date(finaldate, tz = "UTC")
  ### Create calendar based on input parameters
  if (temporal_reso==1/2 && temporal_reso_unit=="month") {
    ### Case : months are cut in two periods : first one from 1 to 15th, second one from 16 to month end
    ### initialisation
    temp_step <- 1
    day(firstdate) <- 01
    month(finaldate)<- month(finaldate)+1
    finaldate <- rollback(finaldate)
    mi_month <- firstdate
    day(mi_month) <- 15
    ### create the first date of each month
    pre_calendar_1 <- seq(firstdate, finaldate, paste(temp_step, temporal_reso_unit))
    ### create the 15th of each month
    pre_calendar_2 <- seq(mi_month, finaldate, paste(temp_step, temporal_reso_unit))
    ### create the calendar end
    end_calendar <- c(pre_calendar_1[length(pre_calendar_1)] + months(temp_step)-days(1))
    ### merge in a calendar, time_start = period beginnig, time_end = period end
    calendar <- data.frame(time_start=sort(c(pre_calendar_1,pre_calendar_2+1)),time_end=sort(c(pre_calendar_1[2:length(pre_calendar_1)]-1,pre_calendar_2, end_calendar)))
  } else {
    ### Other case
    ### create the period of time
    pre_calendar <- seq(firstdate, finaldate, paste(temporal_reso, temporal_reso_unit))
    ### create the calendar end
    switch(temporal_reso_unit,
           "day" = {end_calendar <- pre_calendar[length(pre_calendar)] + days(temporal_reso)-days(1)}, 
           "month" = { end_calendar <- pre_calendar[length(pre_calendar)] + months(temporal_reso)-days(1)},
           "year" = {end_calendar <- pre_calendar[length(pre_calendar)] + years(temporal_reso)-days(1)}
    )
    ### merge in a calendar, time_start = period beginnig, time_end = period end
    calendar <- data.frame(time_start=pre_calendar,time_end=c(pre_calendar[2:length(pre_calendar)]-1, end_calendar))
  }
  
  return(calendar)
}
