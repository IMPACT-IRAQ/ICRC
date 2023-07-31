# PURPOSE: create cleaning log for ICRC 2023 , the input are daily data and kobo form so the output a cleaning log 

# AUTHOR: Mardy Palanadar  | Database assistant 

# DATE CREATED: July-10, 2023

# NOTES: 

library(lubridate)
library(readxl)
library(writexl)
library(stringr)
library(illuminate)
library(purrr)
library(tidyverse)
library(openxlsx)
library(stringr)
library(sf)
library(leaflet)
library(devtools)
library(srvyr)
library(purrr)

icrc_kobo <- read_excel('./input/kobo.xlsx',sheet =1,guess_max = Inf) 
icrc_kobo_choices <- read_excel('./input/kobo.xlsx',sheet =2,guess_max = Inf)
icrc_data <- read_excel('./input/data.xlsx',sheet = 1,guess_max =Inf)
### change camp name from ninawa to erbil 







icrc_data$today_sub <- date(icrc_data$`_submission_time`)

#######################################################################



############################################################################### other checks 
selected_columns <- grep("_other$", names(icrc_data), value = TRUE)
selected_columns <- selected_columns[!sapply(icrc_data[selected_columns], is.numeric)]

# Specify the four additional columns you want to select
additional_columns <- c("start", "end",'enumerator_num','_uuid','gps_point_interview','today_sub')

# Combine the selected columns with the additional columns
columns_to_select <- c(selected_columns, additional_columns)

# Select the columns from the data frame
icrc_data_selected <- icrc_data[columns_to_select]




cl1 <- function(data)
{
  col_names <- names(data)
  col_names_others <- col_names[endsWith(col_names,"_other") & !grepl("/", col_names)]
  pivot_longer(data,col_names_others, names_to = "question", values_to = "old_value") %>% filter(!is.na(old_value)) %>% mutate(issue="other fields (validate and/or translate)") %>% mutate_all(as.character)
}

cl1(icrc_data_selected)



clean1 <- cl1(icrc_data_selected) %>% mutate(new_value='',status='',comment_from_SFO='') %>% select(start,end,enumerator_num,'_uuid',question,issue,old_value,comment_from_SFO,new_value,status,gps_point_interview,today_sub)


# caling question that has other 
quesion_no_other <- clean1$question %>% str_remove('_other')




#seperate type from listname 
separate_type <-  icrc_kobo %>% select(type,name,`label::English (en)`,`label::Arabic (ar)`) %>%  separate(type,sep=" ",c("type","listname")) 

separate_type <- na.omit(separate_type)



# cobine choices in one cell 

group_and_concat <- icrc_kobo_choices %>%
  group_by(list_name) %>%
  summarise(all_choices = paste(name, collapse = " | "))


#combine choices with questions 

choices_questions <- left_join(group_and_concat,separate_type,by=c("list_name"="listname")) %>% select(all_choices,name,`label::English (en)`,`label::Arabic (ar)`) %>% mutate(name=str_c(name,'_other'))



final_cleaning_1 <- left_join(clean1,choices_questions,by=c('question'='name')) %>% select(start,end,'_uuid',enumerator_num,question,`label::English (en)`,`label::Arabic (ar)`,old_value,issue,comment_from_SFO,new_value,status,gps_point_interview,today_sub) %>% rename(question_name=question)

view(final_cleaning_1)






####################################################################### outliers checks


## caling only numeric data 

data_numeric <- icrc_data %>% select(start,end,enumerator_num,'_uuid',today_sub,gps_point_interview, where(~identical(class(.), c("numeric"))) &!contains("/") &!contains("_gps") &!contains("_id") )
                                   


data_pivot <- data_numeric %>% pivot_longer(c(-1,-2,-3,-4,-5,-6),names_to ='questions',values_to = 'values') %>% filter(!is.na(values))




data_groupby_question <- data_pivot %>% group_by(questions) %>% mutate(outlier=rep(list(boxplot.stats(values)$out), length(questions))) %>% rowwise() %>% mutate(is_outlier = values %in% outlier)




cleaning_2 <- data_groupby_question %>% mutate(old_value=values,new_value='',status='',issue='This value seems exceptionally high/low, please check with enumerator/respondent to verify?',comment_from_SFO='') %>% filter(is_outlier==TRUE)  %>%   select(start,end,enumerator_num,'_uuid',comment_from_SFO,question=questions,issue,old_value,new_value,status,gps_point_interview,today_sub)



#unite_phone_number_from_cleaning_2 <-   cleaning_2 %>%  unite(telephone,telephone_number_ki,col = telephone_number,remove =TRUE,na.rm =TRUE)



question_label <- icrc_kobo %>% select(name,`label::English (en)`,`label::Arabic (ar)`)

final_cleaning_2 <- left_join(cleaning_2,question_label,by=c('question'='name'))  %>% select(start,end,'_uuid',enumerator_num,question,`label::English (en)`,`label::Arabic (ar)`,old_value,issue,comment_from_SFO,new_value,status,gps_point_interview,today_sub) %>% rename(question_name=question)

view(final_cleaning_2)




##################################################### combine other with oulier check            

final_cleaning_2$start <- as.character(final_cleaning_2$start)
final_cleaning_2$end <- as.character(final_cleaning_2$end)
final_cleaning_2$old_value <- as.character(final_cleaning_2$old_value)
final_cleaning_2$today_sub <- as.character(final_cleaning_2$today_sub)





######################## duration 


uuids <- list.files("./input/audit")
audit_file_paths <- list.files("./Input/audit", full.names = T) %>% str_c("/audit.csv")


all_audits <- map2_dfr(audit_file_paths, uuids, function(audit_file_path, survey_uuid){
  read_csv(audit_file_path, guess_max = 500, show_col_types = F, col_select = c("event",	"node",	"start",	"end")) %>% mutate(uuid=survey_uuid)
}) %>% filter(event %in% c("group questions", "question") & !str_detect(node, "gps")) %>% mutate(duration_ms = if_else((end-start) > 420000, 420000, (end-start)), duration_s = duration_ms/1000, duration_m = duration_s/60) 

survey_durations <- all_audits %>% group_by(uuid) %>% summarise(survey_duration_ms = sum(duration_ms, na.rm=T), survey_duration_s = sum(duration_s, na.rm=T), survey_duration_m = sum(duration_m, na.rm=T))


icrc_data <- icrc_data %>% left_join(survey_durations %>% select(uuid, survey_duration_m), by = c("_uuid" = "uuid")) 


final_cleaning_3 <-  icrc_data %>%  mutate(new_value='',old_value=survey_duration_m,status="",issue='the duration of surevy is less than 30 or more than 120 minutes',comment_from_SFO='') %>% filter(old_value <30 | old_value > 120) %>%  select(start,end,'_uuid',enumerator_num,old_value,issue,comment_from_SFO,new_value,status,gps_point_interview,today_sub) 


view(final_cleaning_3)



final_cleaning_3$start <- as.character(final_cleaning_3$start)
final_cleaning_3$end <- as.character(final_cleaning_3$end)
final_cleaning_3$old_value <- as.character(final_cleaning_3$old_value)
final_cleaning_3$today_sub <- as.character(final_cleaning_3$today_sub)





########### combine all the cleaning into one cleaning 


final_cleaning_all_with_sub <- bind_rows(final_cleaning_1, final_cleaning_2,final_cleaning_3) %>% mutate_all(as.character) 

final_cleaning_all <- final_cleaning_all_with_sub  %>% select(-today_sub)

view(final_cleaning_all)

write_xlsx(final_cleaning_all,'./Output/cleaning_log_all_2023.xlsx')



##### filter only today date 


#dates <- c("2023-07-26", "2023-07-27","2023-07-28","2023-07-29") #//// filter(today_sub %in% dates) filter(today_sub=="2023-07-11")



final_cleaning_today_data_only <- final_cleaning_all_with_sub %>% filter(today_sub=="2023-07-30") %>% select(-today_sub) #%>% rename('sub_date'='today_sub')

view(final_cleaning_today_data_only)

write_xlsx(final_cleaning_today_data_only,'./Output/cleaning_log_july_30_2023.xlsx')


