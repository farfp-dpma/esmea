{
  library(aws.s3)
  library(bs4Dash)
  library(collapse)
  library(data.table)
  library(DBI)
  library(DT)
  library(expss)
  library(fst)
  library(ggplot2)
  library(glue)
  library(gsubfn)
  library(hablar)
  library(janitor)
  library(lares)
  library(lubridate)
  library(memoise)
  library(openssl)
  library(openxlsx)
  library(plotly)
  library(pool)
  library(readr)
  library(readxl)
  library(RPostgres)
  library(scales)
  library(scales) 
  library(shiny)
  library(shinyalert)
  library(shinycssloaders)
  library(shinyFeedback)
  library(shinyFiles)
  library(shinyjs)
  library(shinymanager)
  library(shinyvalidate)
  library(shinyWidgets)
  library(sodium)
  library(sp)
  library(splitstackshape)
  library(stringi)
  library(stringr)
  library(tidyverse)
  library(zoo)
  library(tmap)
  library(rmapshaper)
  library(sf)
  library(flextable)
  library(fresh)
  library(naniar)
  library(uuid)
  
  shinyOptions(cache = cachem::cache_disk("./bind-cache"))
} ######## LOADING LIBRARIES
{db2 <- 'farmader'
host_db2 <- "maderfar.ch6pgds7fteq.eu-north-1.rds.amazonaws.com"
db_port2 <- '5432'
db_user2 <- "postgres"
db_password2 <- "FAR;2030"
Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIA3FZC6NBQY25AJNWF", "AWS_SECRET_ACCESS_KEY" = "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", "AWS_DEFAULT_REGION" = "eu-north-1")

far_pool <- dbPool(RPostgres::Postgres(), dbname = db2, host=host_db2, port=db_port2, user=db_user2, password=db_password2)
onStop(function() {poolClose(far_pool)})
}########### DATABASE CONNECTION

grafico_barras <- function(data, categoria, valor, meta){
  data <- data %>% mutate(valores := {{valor}}) %>% 
    mutate(categoria := as.character({{categoria}}),
           valor := as.numeric({{valor}}),
           categoria := fct_reorder({{categoria}}, {{valor}}),
           fill := ifelse(valor == max({{valor}}), "Dark", "Light"))
  
  no_y_grid_plot <- ggplot(data, aes(valor, categoria)) +
    geom_col(aes(x = valor, y=categoria, fill  = fill))+
    theme_minimal(base_size = 14)+
    geom_text(
      data = data,
      mapping = aes(x = valor, y = categoria, label = valor),
      hjust = 1,
      nudge_x = -0.1,
      color = 'white',
      fontface = 'bold',
      size = 4.5
    ) +
    scale_fill_manual(values = c('#008000', '#A2AD9C'))+
    geom_text(
      data = data,
      mapping = aes(x = 0, y = categoria, label = categoria),
      hjust = 0,
      nudge_x = 0.25,
      color = 'white',
      fontface = 'bold',
      size = 4.5
    ) +
    geom_vline(xintercept=40,col = "red", lty=2) +
    geom_vline(xintercept = 0) +
    scale_x_continuous(breaks = NULL, expand = expansion(mult = c(0, 0.01))) +
    scale_y_discrete(breaks = NULL) +
    labs(x = element_blank(), y = element_blank()) +
    theme(panel.grid.major.x = element_blank(), panel.grid.minor.x = element_blank(), legend.position = "none")
  no_y_grid_plot
}

bentrains_uploads <- function(designacao_ficheiro) {
   sessions <- read_excel(designacao_ficheiro, sheet = "sessoes", col_types = coltypes_training_sessions) %>% 
    dplyr::filter(!is.na(admin_id) & !is.na(topic_id) & !is.na(date_started)) %>%
    mutate(nome = word(staff_name, 1), apelido = word(staff_name, 2)) %>%
    mutate(nome_limpo = limpar_nomes(ifelse(is.na(nome), 'NULL', nome)),
           apelido_limpo = limpar_nomes(ifelse(is.na(apelido), 'NULL', nome)),
           texto_sessoes = paste0("('", topic_id, "', ", admin_id, ", '", date_started, "')"),
           topic_id = ifelse(topic_id == "NA",NA,topic_id))
  
  texto_sessoes <- glue_sql_collapse(sessions$texto_sessoes , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.sessoes (topic_id, admin_id, date_started) VALUES', texto_sessoes, " ON CONFLICT DO NOTHING;")))
  sessoes_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as session_id, topic_id, date_started, admin_id FROM trained.sessoes WHERE 
                                          admin_id IN (", glue_sql_collapse(sessions$admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND topic_id IN ('",  glue_sql_collapse(sessions$topic_id, sep = "', '", width = Inf, last = ""), "')  
                                          AND date_started IN ('", glue_sql_collapse(sessions$date_started, sep = "', '", width = Inf, last = ""), "');")))
  
  coded_sessions <- sessoes_id %>% left_join(sessions, by = c("date_started", "topic_id", "admin_id"))
  sessions_staff <- coded_sessions %>% mutate(texto_staff = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", staff_id, ")"),
                                              texto_session_staff = paste0("(", staff_id, ", ", session_id, ")"),
                                              texto_session_ends = paste0("('", date_ended, "', ", session_id, ")"),
                                              texto_audience = paste0("('", audience, "', ", session_id, ")"))
  
  df_staff <- sessions_staff %>% dplyr::filter(!is.na(staff_id) & staff_id != "NA")
  df_session_staff <- sessions_staff %>% dplyr::filter(!is.na(staff_id) & staff_id != "NA" & session_id != "NA" & !is.na(session_id))
  
  texto_staff <- glue_sql_collapse(df_staff$texto_staff , sep = ", ", width = Inf, last = "")
  texto_session_staff <- glue_sql_collapse(df_session_staff$texto_session_staff, sep = ", ", width = Inf, last = "")
  texto_session_ends <- glue_sql_collapse(sessions_staff$texto_session_ends, sep = ", ", width = Inf, last = "")
  texto_audience <- glue_sql_collapse(sessions_staff$texto_audience , sep = ", ", width = Inf, last = "")
  
  training_venues <- read_excel(designacao_ficheiro, sheet = "training_venues", col_types =coltypes_training_venues) %>% 
    dplyr::filter(!is.na(admin_id) & !is.na(date_started) & !is.na(topic_id) & topic_id != "0", admin_id != 0) %>% 
    left_join(sessoes_id, by = c("admin_id", "date_started", "topic_id")) %>% 
    mutate(session_id = ifelse(is.na(session_id), 1, session_id))
  # 
  training_locales <- training_venues %>% 
    dplyr::filter(!is.na(povoado) & povoado != "0") %>% mutate(session_id = ifelse(is.na(session_id), 1, session_id)) %>% 
    mutate(texto_venues = paste0("('", povoado, "', ", session_id, ")"))
  
  texto_venues <- glue_sql_collapse(training_locales$texto_venues, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.staff (nome, apelido, id) VALUES', texto_staff, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.session_staff (staff_id, session_id) VALUES', texto_session_staff, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.audience (audience, session_id) VALUES', texto_audience, " ON CONFLICT DO NOTHING;")))
  
  venue_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as venue_id, session_id FROM trained.venues WHERE 
                                          session_id IN (", glue_sql_collapse(training_venues$session_id, sep = ", ", width = Inf, last = ""), ")
                                          AND venue IN ('", glue_sql_collapse(training_venues$povoado, sep = "', '", width = Inf, last = ""), "');")))
  
  
  training_coordinates <- coded_sessions %>% left_join(venue_id, by = c("session_id")) %>% dplyr::filter(!is.na(topic_id) & !is.na(admin_id)) %>% 
    dplyr::bind_rows(data.frame(venue_id = c(9999), latitude = c(-11), longitude = c(40))) %>% 
    dplyr::filter(!is.na(latitude) & latitude != 0 & !is.na(longitude) & longitude != 0) %>% 
    mutate(venue_id = ifelse(is.na(venue_id) | venue_id == "NA", 1, venue_id)) %>% 
    mutate(texto_venue_coordinates = paste0("(", venue_id, ", ", latitude, ", ",  longitude, ")"))
  
  
  texto_venue_coordinates <- glue_sql_collapse(training_coordinates$texto_venue_coordinates, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.venue_coordinates (venue_id, latitude, longitude) VALUES', texto_venue_coordinates, " ON CONFLICT DO NOTHING;")))
  
  trained_topics <- read_excel(designacao_ficheiro, sheet = "topics", col_types = coltypes_traning_topics) %>%  
    dplyr::filter(!is.na(admin_id) & !is.na(topico) & topico != "0") %>%
    left_join(sessoes_id, by = c("admin_id", "date_started", "topic_id")) %>% 
    mutate(texto_trained_topics = paste0("(", session_id,  ", '", topico,  "', '", entidade,  "', '", facilitador, "', ", horas, ")"))
  
  texto_trained_topics <- glue_sql_collapse(trained_topics$texto_trained_topics, sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.topicos (session_id, topico, entidade, facilitador, horas) VALUES', texto_trained_topics, " ON CONFLICT DO NOTHING;")))
  
  dataset <- read_excel(designacao_ficheiro, sheet = "beneficiarios", col_types = coltypes_treinamento_beneficiarios) %>%  
    dplyr::filter(nome != "0" & !is.na(nome) & !is.na(apelido) & apelido != 0 & !is.na(people_admin_id) & people_admin_id != 0) %>% 
    mutate(nome_limpo = limpar_nomes(nome), apelido_limpo = limpar_nomes(apelido)) %>% 
    mutate(texto_beneficiaries = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", people_admin_id,")"))
  
  texto_beneficiaries <- glue_sql_collapse(dataset$texto_beneficiaries , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pessoas (nome, apelido, admin_id) VALUES', texto_beneficiaries, " ON CONFLICT DO NOTHING;")))
  beneficiaries_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as people_id, nome AS nome_limpo, apelido AS apelido_limpo, admin_id as people_admin_id FROM beneficiaries.pessoas WHERE 
                                          admin_id IN (", glue_sql_collapse(as.character(dataset$people_admin_id), sep = ", ", width = Inf, last = ""), ")
                                          AND nome IN ('",  glue_sql_collapse(dataset$nome_limpo, sep = "', '", width = Inf, last = ""), "')  
                                          AND apelido IN ('", glue_sql_collapse(dataset$apelido_limpo, sep = "', '", width = Inf, last = ""), "');")))
  
  beneficiaries_coded <- dataset %>% mutate(people_admin_id = as.numeric(people_admin_id)) %>% 
    left_join(beneficiaries_id, by = c("people_admin_id", "nome_limpo", "apelido_limpo")) %>% 
    left_join(coded_sessions %>% select(-nome, -apelido), by = c("admin_id", "date_started", "topic_id")) %>% 
    mutate(session_id = ifelse(is.na(session_id), 1, session_id)) %>% 
    mutate(texto_bentrains = paste0("(", people_id, ", ", session_id, ")")) %>% 
    
    dplyr::filter(!is.na(topic_id))
  
  texto_bentrains_papel <- beneficiaries_coded %>% dplyr::filter(!is.na(role) & role != '0') %>% 
    mutate(texto_bentrains_role = paste0("('", role, "', ", people_id, ")"))
  
  texto_bentrains <- glue_sql_collapse(beneficiaries_coded$texto_bentrains , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.beneficiarios (people_id, session_id) VALUES', texto_bentrains, " ON CONFLICT DO NOTHING;")))
  
  tryCatch({
    texto_bentrains_role <- glue_sql_collapse(texto_bentrains_papel$texto_bentrains_role , sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.papel_social (papel, people_id) VALUES', texto_bentrains_role, " ON CONFLICT DO NOTHING;")))
  }, error = function(e) {return(0)})
  
  tryCatch({
    grupos <- beneficiaries_coded %>% dplyr::filter(!is.na(entidade) & entidade != '0' & !is.na(admin_id)) %>% 
      mutate(designacao_limpa = limpar_nomes(entidade)) %>% 
      distinct(designacao_limpa, admin_id, .keep_all = TRUE) %>% 
      dplyr::bind_rows(data.frame(designacao_limpa = c("TESTE"), admin_id = c(999999))) %>% 
      mutate(texto_msme = paste0("('", designacao_limpa, "', ", admin_id, ")"))
    
    texto_msme <- glue_sql_collapse(grupos$texto_msme , sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO msme.msme (designacao, admin_id) VALUES', texto_msme, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  
  msme_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as msme_id, designacao AS entidade, admin_id FROM msme.msme WHERE 
                                          admin_id IN (", glue_sql_collapse(grupos$admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND designacao IN ('", glue_sql_collapse(grupos$designacao_limpa, sep = "', '", width = Inf, last = ""), "');")))
  
  beneficiaries_coded <- beneficiaries_coded %>% 
    left_join(msme_id, by = c("entidade", "admin_id"))           
  
  beneficiaries_details <- beneficiaries_coded %>% 
    dplyr::bind_rows(data.frame(msme_id = c(1), people_id = c(917), ano = c(1850), sexo = c("TESTE"), phone = c("TESTE"), povoado = c("TESTE"))) %>% 
    mutate(texto_membership = paste0("(", people_id, ", ", msme_id, ")"),
           texto_idades = paste0("(", people_id, ", ", ano, ")"),
           texto_sexo = paste0("(", people_id, ", '", sexo, "')"),
           texto_contacto = paste0("(", people_id, ", '", phone, "')"),
           texto_povoado = paste0("(", people_id, ", '", povoado, "')")
    )
  
  df_membership <- beneficiaries_details %>% dplyr::filter(!is.na(msme_id) & msme_id != 0)
  df_idades <- beneficiaries_details %>% dplyr::filter(!is.na(ano) & ano != 0)
  df_sexo <- beneficiaries_details %>% dplyr::filter(!is.na(sexo) & sexo != 0)
  df_contacto <- beneficiaries_details %>% dplyr::filter(!is.na(phone) & phone != 0)
  df_povoado <- beneficiaries_details %>% dplyr::filter(!is.na(povoado) & povoado!= 0)
  
  # texto_membership <- glue_sql_collapse(df_membership$texto_membership , sep = ", ", width = Inf, last = "")
  texto_idades <- glue_sql_collapse(df_idades$texto_idades , sep = ", ", width = Inf, last = "")
  texto_sexo <- glue_sql_collapse(df_sexo$texto_sexo , sep = ", ", width = Inf, last = "")
  texto_contacto <- glue_sql_collapse(df_contacto$texto_contacto , sep = ", ", width = Inf, last = "")
  texto_povoado <- glue_sql_collapse(df_povoado$texto_povoado, sep = ", ", width = Inf, last = "")
  
  
  tryCatch({
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.idades (person_id, ano) VALUES', texto_idades, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  tryCatch({
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.contactos (people_id, contacto) VALUES', texto_contacto, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  # dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.membership (people_id, msme_id) VALUES', texto_membership, " ON CONFLICT DO NOTHING;")))
  
  tryCatch({
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.sexo (person_id, sexo) VALUES', texto_sexo, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  tryCatch({
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.povoados (person_id, povoado) VALUES', texto_povoado, " ON CONFLICT DO NOTHING;")))
  }, error = function(e) {return(0)})
  
  tryCatch({
    texto_membership <- glue_sql_collapse(df_membership$texto_membership , sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.membership (people_id, msme_id) VALUES', texto_membership, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  
  tryCatch({
    disability <- beneficiaries_coded %>% select(people_id, pwd)
    disability <- cSplit(disability, "pwd", ";")
    disability <- disability %>% 
      pivot_longer(cols = starts_with("pwd"), names_to = "counting", values_to = "disabled") %>% 
      dplyr::select(-counting) %>% dplyr::filter(!is.na(disabled) & disabled != '0') %>% 
      mutate(disability = parse_number(disabled))
    
    disability$hh_member <- gsub("[0-9]+", "", disability$disabled)
    disability <- disability %>% 
      dplyr::bind_rows(data.frame(people_id = c(917), disability = c(1), hh_member = "Nenhum")) %>% 
      mutate(texto = paste0("(", people_id, ", '",disability,"', '",hh_member,"')"))
    texto_disability <- glue_sql_collapse(disability$texto, sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pwd (people_id, hh_member, disability) VALUES', texto_disability, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  
  tryCatch({
    vulnerability <- beneficiaries_coded %>% select(people_id, pwv)
    vulnerability <- cSplit(vulnerability, "pwv", ";")
    vulnerability <- vulnerability %>% 
      pivot_longer(cols = starts_with("pwv"), names_to = "counting", values_to = "vulnerable") %>% 
      dplyr::select(-counting) %>% dplyr::filter(!is.na(vulnerable) & vulnerable != '0') %>% 
      mutate(vulnerability = parse_number(vulnerable))
    vulnerability$hh_member <- gsub("[0-9]+", "", vulnerability$vulnerable)
    vulnerability <- vulnerability %>% 
      dplyr::bind_rows(data.frame(people_id = c(917), vulnerability = c(1), hh_member = "Nenhum")) %>% 
      mutate(texto = paste0("(", people_id, ", '",vulnerability,"', '",hh_member,"')"))
    texto_vulnerability <- glue_sql_collapse(vulnerability$texto, sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pwv (people_id, hh_member, vulnerability) VALUES', texto_vulnerability, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  
  #############  CADEIAS DE VALOR
  
  tryCatch({
    sessoes_cadeias <- coded_sessions %>% select(cadeias, session_id)
    sessoes_cadeias <- cSplit(sessoes_cadeias, "cadeias", ";")
    sessoes_cadeias$cadeias <- gsub(",", ";", sessoes_cadeias$cadeias)
    sessoes_cadeias <- sessoes_cadeias %>%
      pivot_longer(cols = starts_with("cadeias_"), values_to = "cadeia", names_to = "names_to") %>% 
      dplyr::filter(!is.na(cadeia)) %>% dplyr::select(session_id, cadeia) %>% 
      mutate(texto_cadeias = paste0("(", session_id, ", '",cadeia,"')"))
    texto_cadeias <- glue_sql_collapse(sessoes_cadeias$texto_cadeias, sep = ", ", width = Inf, last = "")
    dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.cadeias (session_id, cadeia) VALUES', texto_cadeias, " ON CONFLICT DO NOTHING;")))
    
  }, error = function(e) {return(0)})
  
  
  
  
  
  
  
  
  
  
  
  
}

deliveries_uploads <- function(designacao_ficheiro) {
  dataset <- read_excel(designacao_ficheiro, sheet = "beneficiaries", col_types = coltypes_deliveries_beneficiaries) %>%  
    dplyr::filter(nome != "0" & !is.na(nome) & !is.na(apelido) & apelido != 0 & !is.na(people_admin_id) & people_admin_id != 0) %>% 
    mutate(nome_limpo = limpar_nomes(nome),
           apelido_limpo = limpar_nomes(apelido)) %>% 
    mutate(texto_beneficiaries = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", people_admin_id,")"))
  texto_beneficiaries <- glue_sql_collapse(dataset$texto_beneficiaries , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pessoas (nome, apelido, admin_id) VALUES', texto_beneficiaries, " ON CONFLICT DO NOTHING;")))
  
  
  agentes_responsaveis <- read_excel(designacao_ficheiro, sheet = "delivery_events", col_types = coltypes_deliveries_events) %>%  
    dplyr::filter(!is.na(nome_rep) & nome_rep != '0') %>% dplyr::filter(!is.na(admin_id)) %>% 
    select(admin_id, items, delivery_date, nome_rep, apelido_rep) %>% 
    mutate(nome_limpo = limpar_nomes(nome_rep), apelido_limpo = limpar_nomes(apelido_rep)) %>% 
    mutate(texto_agente_responsavel = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", admin_id,")"))
  texto_agente_responsavel <- glue_sql_collapse(agentes_responsaveis$texto_agente_responsavel , sep = ", ", width = Inf, last = "")
  
  print(texto_agente_responsavel)
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO  beneficiaries.pessoas (nome, apelido, admin_id) VALUES', texto_agente_responsavel, " ON CONFLICT DO NOTHING;")))
  
  
  beneficiaries_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as people_id, nome AS nome_limpo, apelido AS apelido_limpo, admin_id as people_admin_id FROM beneficiaries.pessoas WHERE 
                                          admin_id IN (", glue_sql_collapse(as.character(dataset$people_admin_id), sep = ", ", width = Inf, last = ""), ")
                                          AND nome IN ('",  glue_sql_collapse(dataset$nome_limpo, sep = "', '", width = Inf, last = ""), "')  
                                          AND apelido IN ('", glue_sql_collapse(dataset$apelido_limpo, sep = "', '", width = Inf, last = ""), "') OR
                                          
                                          (admin_id IN (", glue_sql_collapse(as.character(agentes_responsaveis$admin_id), sep = ", ", width = Inf, last = ""), ")
                                          AND nome IN ('",  glue_sql_collapse(agentes_responsaveis$nome_limpo, sep = "', '", width = Inf, last = ""), "')  
                                          AND apelido IN ('", glue_sql_collapse(agentes_responsaveis$apelido_limpo, sep = "', '", width = Inf, last = ""), "'));")))
  
  msme <- read_excel(designacao_ficheiro, sheet = "msme", col_types = coltypes_deliveries_msme) %>% 
    dplyr::filter(!is.na(designacao) & designacao != "" & !is.na(id) & id != "" & !is.na(admin_id)) %>% 
    mutate(designacao_limpa = limpar_nomes(designacao)) %>% 
    distinct(designacao_limpa, admin_id, .keep_all = TRUE)
  
  beneficiaries_coded <- dataset %>% mutate(staff = staff_id) %>% 
    mutate(people_admin_id = as.numeric(people_admin_id)) %>% 
    left_join(beneficiaries_id, by = c("people_admin_id", "nome_limpo", "apelido_limpo")) %>%
    mutate(designacao = group_id) %>% 
    mutate(texto_beneficiaries_staff = paste0("(", people_id, ", ", staff_id, ")"))
  
  benef_groups <- beneficiaries_coded %>% dplyr::filter(!is.na(group_id) & group_id != '0' & !is.na(admin_id)) %>% 
    mutate(designacao_limpa = limpar_nomes(group_id)) %>% 
    distinct(designacao_limpa, admin_id, .keep_all = TRUE)
  
  grupos <- dplyr::bind_rows(msme[, c("designacao_limpa", "admin_id", "staff_id")], benef_groups[, c("designacao_limpa", "admin_id", "staff_id")]) %>% distinct_all() %>% 
    mutate(texto_msme = paste0("('", designacao_limpa, "', ", admin_id, ")"))
  
  texto_msme <- glue_sql_collapse(grupos$texto_msme , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO msme.msme (designacao, admin_id) VALUES', texto_msme, " ON CONFLICT DO NOTHING;")))
  
  msme_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as msme_id, designacao AS designacao_limpa, admin_id FROM msme.msme WHERE 
                                          admin_id IN (", glue_sql_collapse(grupos$admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND designacao IN ('", glue_sql_collapse(grupos$designacao_limpa, sep = "', '", width = Inf, last = ""), "');")))
  
  beneficiaries_coded <- beneficiaries_coded %>% 
    mutate(designacao_limpa = limpar_nomes(designacao)) %>% 
    left_join(msme_id, by = c("designacao_limpa", "admin_id"))
  
  beneficiaries_details <- beneficiaries_coded %>% 
    dplyr::filter(people_id != 0 & !is.na(people_id)) %>%
    mutate(texto_membership = paste0("(", people_id, ", ", msme_id, ")"),
           texto_idades = paste0("(", people_id, ", ", ano, ")"),
           texto_sexo = paste0("(", people_id, ", '", sexo, "')"),
           texto_contacto = paste0("(", people_id, ", '", contacts, "')"),
           texto_povoado = paste0("(", people_id, ", '", povoado, "')")
    )
  df_membership <- beneficiaries_details %>% dplyr::filter(!is.na(msme_id) & msme_id != 0)
  df_idades <- beneficiaries_details %>% dplyr::filter(!is.na(ano) & ano != 0)
  df_sexo <- beneficiaries_details %>% dplyr::filter(!is.na(sexo) & sexo != 0)
  df_contacto <- beneficiaries_details %>% dplyr::filter(!is.na(contacts) & contacts != 0)
  df_povoado <- beneficiaries_details %>% dplyr::filter(!is.na(povoado) & povoado != 0)
  
  texto_membership <- glue_sql_collapse(df_membership$texto_membership , sep = ", ", width = Inf, last = "")
  texto_idades <- glue_sql_collapse(df_idades$texto_idades , sep = ", ", width = Inf, last = "")
  texto_sexo <- glue_sql_collapse(df_sexo$texto_sexo , sep = ", ", width = Inf, last = "")
  texto_contacto <- glue_sql_collapse(df_contacto$texto_contacto , sep = ", ", width = Inf, last = "")
  texto_povoado <- glue_sql_collapse(df_povoado$texto_povoado, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.membership (people_id, msme_id) VALUES', texto_membership, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.idades (person_id, ano) VALUES', texto_idades, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.contactos (people_id, contacto) VALUES', texto_contacto, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.sexo (person_id, sexo) VALUES', texto_sexo, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.povoados (person_id, povoado) VALUES', texto_povoado, " ON CONFLICT DO NOTHING;")))
  
  texto_beneficiaries_staff <- beneficiaries_coded %>% select(people_id, staff_id) %>% 
    dplyr::filter(!is.na(staff_id) & staff_id != 0 & people_id != 0 & !is.na(people_id)) %>% 
    mutate(texto_beneficiaries_staff = paste0("(", people_id, ", ", staff_id, ")"))
  texto_beneficiaries_staff <- glue_sql_collapse(texto_beneficiaries_staff$texto_beneficiaries_staff , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.staff (people_id, staff_id) VALUES', texto_beneficiaries_staff, " ON CONFLICT DO NOTHING;")))
  
  disability <- beneficiaries_coded %>% select(people_id, pwd)
  disability <- cSplit(disability, "pwd", ";")
  disability <- disability %>% 
    pivot_longer(cols = starts_with("pwd"), names_to = "counting", values_to = "disabled") %>% 
    dplyr::select(-counting) %>% dplyr::filter(!is.na(disabled) & disabled != '0') %>% 
    mutate(disability = parse_number(disabled))
  disability$hh_member <- gsub("[0-9]+", "", disability$disabled)
  disability <- disability %>% mutate(texto = paste0("(", people_id, ", '",disability,"', '",hh_member,"')"))
  
  vulnerability <- beneficiaries_coded %>% select(people_id, pwv)
  vulnerability <- cSplit(vulnerability, "pwv", ";")
  vulnerability <- vulnerability %>% 
    pivot_longer(cols = starts_with("pwv"), names_to = "counting", values_to = "vulnerable") %>% 
    dplyr::select(-counting) %>% dplyr::filter(!is.na(vulnerable) & vulnerable != '0') %>% 
    mutate(vulnerability = parse_number(vulnerable))
  vulnerability$hh_member <- gsub("[0-9]+", "", vulnerability$vulnerable)
  vulnerability <- vulnerability %>% mutate(texto = paste0("(", people_id, ", '",vulnerability,"', '",hh_member,"')"))
  
  texto_vulnerability <- glue_sql_collapse(vulnerability$texto, sep = ", ", width = Inf, last = "")
  texto_disability <- glue_sql_collapse(disability$texto, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pwd (people_id, hh_member, disability) VALUES', texto_disability, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.pwv (people_id, hh_member, vulnerability) VALUES', texto_vulnerability, " ON CONFLICT DO NOTHING;")))
  
  delivery_events <- read_excel(designacao_ficheiro, sheet = "delivery_events", col_types = coltypes_delivery_events) %>% 
    dplyr::filter(!is.na(admin_id) & !is.na(items) & !is.na(delivery_date)) %>% 
    mutate(nome = word(staff_name, 1), apelido = word(staff_name, 2)) %>% 
    mutate(nome = ifelse(is.na(nome), 'NULL', nome), 
           apelido = ifelse(is.na(apelido), 'NULL', nome)) %>% 
    mutate(texto_eventos = paste0("('", items, "', ", admin_id, ", '", delivery_date, "')"),
           texto_pessoal = paste0("('", nome, "', '", apelido, "', ", staff_id, ")"),
           texto_contacto_pessoal = paste0("('", staff_contact, "', ", staff_id, ")"))
  
  texto_delivery_events <- glue_sql_collapse(delivery_events$texto_eventos , sep = ", ", width = Inf, last = "")
  texto_pessoal <- glue_sql_collapse(delivery_events$texto_pessoal , sep = ", ", width = Inf, last = "")
  texto_contacto_pessoal <- glue_sql_collapse(delivery_events$texto_contacto_pessoal , sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.eventos (items, admin_id, delivered) VALUES', texto_delivery_events, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.staff (nome, apelido, id) VALUES', texto_pessoal, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.nuit_contacts (contacto, nuit) VALUES', texto_contacto_pessoal, " ON CONFLICT DO NOTHING;")))
  
  delivery_events_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as event_id, items, delivered as delivery_date, admin_id FROM deliveries.eventos WHERE 
                                          admin_id IN (", glue_sql_collapse(delivery_events$admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND items IN ('",  glue_sql_collapse(delivery_events$items, sep = "', '", width = Inf, last = ""), "')  
                                          AND delivered IN ('", glue_sql_collapse(delivery_events$delivery_date, sep = "', '", width = Inf, last = ""), "');")))
  delivery_events_coded <- delivery_events %>% dplyr::left_join(delivery_events_id, by = c("items", "delivery_date", "admin_id"))
  
  delivery_events_cadeias <- delivery_events_coded %>% select(event_id, cadeia_valor)
  delivery_events_cadeias <- cSplit(delivery_events_cadeias, "cadeia_valor", ";")
  delivery_events_cadeias <- delivery_events_cadeias %>% 
    pivot_longer(cols = starts_with("cadeia_valor"), names_to = "counting", values_to = "cadeia") %>% 
    dplyr::select(-counting) %>% dplyr::filter(!is.na(cadeia) & cadeia != '0') %>% 
    mutate(texto_evento_cadeia = paste0("('", cadeia, "', ", event_id, ")"))
  texto_evento_cadeia <- glue_sql_collapse(delivery_events_cadeias$texto_evento_cadeia , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.cadeias (cadeia, event_id) VALUES', texto_evento_cadeia, " ON CONFLICT DO NOTHING;")))
  # print(agentes_responsaveis_coded)
  agentes_responsaveis_coded <- agentes_responsaveis %>% 
    left_join(delivery_events_id, by = c("admin_id", "items", "delivery_date")) %>% 
    left_join(beneficiaries_id %>% mutate(admin_id = people_admin_id), by = c("admin_id", "nome_limpo", "apelido_limpo")) %>% 
    dplyr::filter(!is.na(event_id) & !is.na(people_id)) %>% 
    mutate(agentes_responsaveis_coded = paste0("(", people_id, ", ", event_id, ")"))
  agentes_responsaveis_coded <- glue_sql_collapse(agentes_responsaveis_coded$agentes_responsaveis_coded , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.agente (people_id, event_id) VALUES', agentes_responsaveis_coded, " ON CONFLICT DO NOTHING;")))
}











techtrains_uploads <- function(designacao_ficheiro) {
  
  sessions <- read_excel(designacao_ficheiro, sheet = "sessoes", col_types = coltypes_training_sessions) %>% 
    dplyr::filter(!is.na(admin_id) & !is.na(topic_id) & !is.na(date_started)) %>%
    mutate(nome = word(staff_name, 1), apelido = word(staff_name, 2)) %>%
    mutate(nome_limpo = limpar_nomes(ifelse(is.na(nome), 'NULL', nome)),
           apelido_limpo = limpar_nomes(ifelse(is.na(apelido), 'NULL', nome))) %>%
    mutate(texto_sessoes = paste0("('", topic_id, "', ", admin_id, ", '", date_started, "')")) %>% 
    mutate(topic_id = ifelse(topic_id == "NA",NA,topic_id)) %>%
    filter(!is.na(topic_id))
  
  texto_sessoes <- glue_sql_collapse(sessions$texto_sessoes , sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.sessoes (topic_id, admin_id, date_started) VALUES', texto_sessoes, " ON CONFLICT DO NOTHING;")))
  sessoes_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as session_id, topic_id, date_started, admin_id FROM trained.sessoes WHERE 
                                          admin_id IN (", glue_sql_collapse(sessions$admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND topic_id IN ('",  glue_sql_collapse(sessions$topic_id, sep = "', '", width = Inf, last = ""), "')  
                                          AND date_started IN ('", glue_sql_collapse(sessions$date_started, sep = "', '", width = Inf, last = ""), "');")))
  
  coded_sessions <- sessoes_id %>% left_join(sessions, by = c("date_started", "topic_id", "admin_id"))
  sessions_staff <- coded_sessions %>% mutate(texto_staff = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", staff_id, ")"),
                                              texto_session_staff = paste0("(", staff_id, ", ", session_id, ")"),
                                              texto_audience = paste0("('", audience, "', ", session_id, ")"),
                                              texto_session_ends = paste0("('", date_ended, "', ", session_id, ")"))
  
  staff_df <- sessions_staff %>% dplyr::filter(!is.na(staff_id) & !is.na(nome_limpo) & !is.na(apelido_limpo)) 
  session_end_df <- sessions_staff %>% dplyr::filter(!is.na(session_id) & !is.na(date_ended)) 
  
  sessions_staffing <- sessions_staff %>% dplyr::filter(!is.na(staff_id) & !is.na(session_id))
  
  texto_staff <- glue_sql_collapse(staff_df$texto_staff , sep = ", ", width = Inf, last = "")
  texto_session_staff <- glue_sql_collapse(sessions_staffing$texto_session_staff, sep = ", ", width = Inf, last = "")
  texto_session_ends <- glue_sql_collapse(session_end_df$texto_session_ends, sep = ", ", width = Inf, last = "")
  texto_audience <- glue_sql_collapse(sessions_staff$texto_audience , sep = ", ", width = Inf, last = "")
  
  training_venues <- read_excel(designacao_ficheiro, sheet = "training_venues", col_types =coltypes_training_venues) %>% 
    dplyr::filter(!is.na(admin_id) & !is.na(date_started) & !is.na(topic_id) & topic_id != "0", admin_id != 0) %>% 
    left_join(sessoes_id, by = c("admin_id", "date_started", "topic_id"))
  # 
  training_locales <- training_venues %>% 
    dplyr::filter(!is.na(povoado) & povoado != "0") %>%
    mutate(texto_venues = paste0("('", povoado, "', ", session_id, ")"))
  
  texto_venues <- glue_sql_collapse(training_locales$texto_venues, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.staff (nome, apelido, id) VALUES', texto_staff, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.session_staff (staff_id, session_id) VALUES', texto_session_staff, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.session_ends (end_date, session_id) VALUES', texto_session_ends, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.venues (venue, session_id) VALUES', texto_venues, " ON CONFLICT DO NOTHING;")))
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.audience(audience, session_id) VALUES', texto_audience, " ON CONFLICT DO NOTHING;")))
  
  venue_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as venue_id, session_id FROM trained.venues WHERE 
                                          session_id IN (", glue_sql_collapse(training_venues$session_id, sep = ", ", width = Inf, last = ""), ")
                                          AND venue IN ('", glue_sql_collapse(training_venues$povoado, sep = "', '", width = Inf, last = ""), "');")))
  
  training_coordinates <- coded_sessions %>% left_join(venue_id, by = c("session_id")) %>%
    dplyr::bind_rows(data.frame(venue_id = c(9999), latitude = c(-11), longitude = c(40))) %>% 
    dplyr::filter(!is.na(latitude) & latitude != 0 & !is.na(longitude) & longitude != 0) %>% 
    mutate(texto_venue_coordinates = paste0("(", venue_id, ", ", latitude, ", ",  longitude, ")"))
  
  texto_venue_coordinates <- glue_sql_collapse(training_coordinates$texto_venue_coordinates, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.venue_coordinates (venue_id, latitude, longitude) VALUES', texto_venue_coordinates, " ON CONFLICT DO NOTHING;")))
  
  
  trained_topics <- read_excel(designacao_ficheiro, sheet = "topics", col_types = coltypes_traning_topics) %>%  
    dplyr::filter(!is.na(admin_id) & !is.na(topico) & topico != "0") %>%
    left_join(sessoes_id, by = c("admin_id", "date_started", "topic_id")) %>% 
    dplyr::filter(!is.na(session_id)) %>% 
    mutate(texto_trained_topics = paste0("(", session_id,  ", '", topico,  "', '", entidade,  "', '", facilitador, "', ", horas, ")"))
  
  texto_trained_topics <- glue_sql_collapse(trained_topics$texto_trained_topics, sep = ", ", width = Inf, last = "")
  
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.topicos (session_id, topico, entidade, facilitador, horas) VALUES', texto_trained_topics, " ON CONFLICT DO NOTHING;")))
  
  staff <- read_excel(designacao_ficheiro, sheet = "tecnicos", col_types = coltypes_treinamento_tecnicos) %>%  
    dplyr::filter(nome != "0" & !is.na(nome) & !is.na(apelido) & apelido != 0 & !is.na(people_admin_id) & people_admin_id != 0 & !is.na(topic_id)) %>% 
    mutate(nome_limpo = limpar_nomes(nome), apelido_limpo = limpar_nomes(apelido)) %>% 
    dplyr::filter(facilitador != "SIM") %>% 
    mutate(texto_tecnicos = paste0("('", nome_limpo, "', '", apelido_limpo, "', ", people_admin_id,")"))
  
  texto_trained_staff <- glue_sql_collapse(staff$texto_tecnicos, sep = ", ", width = Inf, last = "")
  dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.staff (nome, apelido, admin_id) VALUES', texto_trained_staff, " ON CONFLICT DO NOTHING;")))
  
  staff_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as staff_id, nome AS nome_limpo, apelido AS apelido_limpo, admin_id AS people_admin_id FROM staff.staff WHERE 
                                          admin_id IN (", glue_sql_collapse(staff$people_admin_id, sep = ", ", width = Inf, last = ""), ")
                                          AND nome IN ('",  glue_sql_collapse(staff$nome_limpo, sep = "', '", width = Inf, last = ""), "')  
                                          AND apelido IN ('", glue_sql_collapse(staff$apelido_limpo, sep = "', '", width = Inf, last = ""), "');")))
  
  staff_coded <- staff %>% 
    left_join(staff_id, by = c("people_admin_id", "nome_limpo", "apelido_limpo")) %>% 
    left_join(coded_sessions %>% select(topic_id, session_id, admin_id, date_started), by = c("topic_id", "admin_id", "date_started")) %>% 
    mutate(texto_techtrains = paste0("(", staff_id, ", ", session_id, ")"))
  
  sexo_df <- staff_coded %>% dplyr::filter(sexo != "0" & !is.na(sexo)) %>% 
    dplyr::bind_rows(data.frame(staff_id = c(1), sexo = c("N"))) %>% 
    mutate(texto = paste0("(", staff_id, ", '", sexo, "')"))
  
  role_df <- staff_coded %>% dplyr::filter(role != "0" & !is.na(role)) %>% 
    dplyr::bind_rows(data.frame(staff_id = c(1), role = c("N"))) %>% 
    mutate(texto = paste0("(", staff_id, ", '", role, "')"))
  
  entidade_df<- staff_coded %>% dplyr::filter(entidade != "0" & !is.na(entidade)) %>% 
    dplyr::bind_rows(data.frame(staff_id = c(1), role = c("N"))) %>% 
    mutate(texto = paste0("(", staff_id, ", '", entidade, "')"))
  
  email_df<- staff_coded %>% dplyr::filter(email != "0" & !is.na(email)) %>% 
    dplyr::bind_rows(data.frame(staff_id = c(1), role = c("N"))) %>% 
    mutate(texto = paste0("(", staff_id, ", '", email, "')"))
  
  phone_df<- staff_coded %>% dplyr::filter(phone != "0" & !is.na(phone)) %>% 
    dplyr::bind_rows(data.frame(staff_id = c(1), role = c("N"))) %>% 
    mutate(texto = paste0("(", staff_id, ", '", phone, "')"))
  
  texto_techtrains <- glue_sql_collapse(staff_coded$texto_techtrains, sep = ", ", width = Inf, last = "")
  
  texto_staff_sexo <- glue_sql_collapse(sexo_df$texto, sep = ", ", width = Inf, last = "")
  texto_staff_entidade <- glue_sql_collapse(entidade_df$texto, sep = ", ", width = Inf, last = "")
  texto_staff_role <- glue_sql_collapse(role_df$texto, sep = ", ", width = Inf, last = "")
  texto_staff_email <- glue_sql_collapse(email_df$texto, sep = ", ", width = Inf, last = "")
  texto_staff_phone <- glue_sql_collapse(phone_df$texto, sep = ", ", width = Inf, last = "")
  
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO trained.staff (staff_id, session_id) VALUES', texto_techtrains, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.sexo (staff_id, sexo) VALUES', texto_staff_sexo, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.entidades (staff_id, entidade) VALUES', texto_staff_entidade, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.roles (staff_id, papel) VALUES', texto_staff_role, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.emails (staff_id, email) VALUES', texto_staff_email, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
  tryCatch({dbGetQuery(far_pool, SQL(paste0('INSERT INTO staff.contactos (staff_id, contacto) VALUES', texto_staff_phone, " ON CONFLICT DO NOTHING;"))) }, error = function(e) {return(0)})
}






limpar_nomes <- function(variavel){
  unwanted_array = list(    'Š'='S', 'š'='s', 'Ž'='Z', 'ž'='z', 'À'='A', 'Á'='A', 'Â'='A', 'Ã'='A', 'Ä'='A', 'Å'='A', 'Æ'='A', 'Ç'='C', 'È'='E', 'É'='E',
                            'Ê'='E', 'Ë'='E', 'Ì'='I', 'Í'='I', 'Î'='I', 'Ï'='I', 'Ñ'='N', 'Ò'='O', 'Ó'='O', 'Ô'='O', 'Õ'='O', 'Ö'='O', 'Ø'='O', 'Ù'='U',
                            'Ú'='U', 'Û'='U', 'Ü'='U', 'Ý'='Y', 'Þ'='B', 'ß'='Ss', 'à'='a', 'á'='a', 'â'='a', 'ã'='a', 'ä'='a', 'å'='a', 'æ'='a', 'ç'='c',
                            'è'='e', 'é'='e', 'ê'='e', 'ë'='e', 'ì'='i', 'í'='i', 'î'='i', 'ï'='i', 'ð'='o', 'ñ'='n', 'ò'='o', 'ó'='o', 'ô'='o', 'õ'='o',
                            'ö'='o', 'ø'='o', 'ù'='u', 'ú'='u', 'û'='u', 'ý'='y', 'ý'='y', 'þ'='b', 'ÿ'='y' )
  corrected <- str_to_title(tolower((gsubfn(paste(names(unwanted_array),collapse='|'), unwanted_array, variavel))))
}












beneficiary_counting_quantidade <- function(data, Description = "cadeia", column_labs = "age_group", sexo = "sexo", summary_column = "people_id", faixa = "faixa", ano = "ano", provincia = "provincia") {
  
  data <- dbGetQuery(far_pool, paste0("SELECT * FROM vistas.outreach WHERE intervencao = 'Vacinação Contra a Newcastle'"))
  data <- data %>% select(provincia, Description = cadeia, sexo, everything())
  out <- data %>%
    group_by(provincia, Description, sexo) %>%
    summarise(beneficiaries = n_distinct(people_id, na.rm = TRUE), .groups = 'drop') %>%
    pivot_wider(names_from = sexo, values_from = beneficiaries) %>%
    group_by(provincia) %>%
    group_modify(~ .x %>% adorn_totals(where = "row")) %>%
    ungroup %>%
    split_columns(columns = 1)
  
  out$provincia[out$provincia== ""] <- NA
  out <- out %>% fill(provincia, .direction = "down") %>% adorn_totals("col") %>% 
    dplyr::filter(Total > 0) %>% 
    mutate(order = ifelse(Description == "Total", Total*1.1,Total)) %>% 
    arrange(order) %>% select(-order)
  out$cadeias_province <- paste0(out$provincia, out$Description)
  age_groups <- data %>% dplyr::filter(ano > 0 & !is.na(ano))
  age_groups$cadeias_province <- paste0(age_groups$provincia, age_groups$Description)

  # YOUTH PROVINCE AND VALUE CHAIN
  cadeias_provinces <- age_groups %>% group_by(cadeias_province, faixa) %>%
    summarize(beneficiaries = n_distinct(people_id)) %>% 
    spread(faixa, -cadeias_province) %>% adorn_totals("col") %>% mutate(youth_pct = Jovens/Total) %>% select(cadeias_province, youth_pct)
  # YOUTH PER VALUE CHAIN
  cadeias <- age_groups %>% group_by(Description, faixa) %>% 
    summarise(beneficiaries = n_distinct(people_id)) %>% spread(faixa, - Description) %>% adorn_totals("col") %>%
    mutate(jovens_cadeias = Jovens/Total) %>% select(Description, jovens_cadeias)
  ## YOUTH PER PROVINCE
  provincias <- age_groups %>% group_by(provincia, faixa) %>% 
    summarise(beneficiaries = n_distinct(people_id)) %>% spread(faixa, - provincia) %>% adorn_totals("col") %>%
    mutate(jovens_provincias = Jovens/Total) %>% select(provincia, jovens_provincias) %>% dplyr::filter(!is.na(provincia))
  ## COMPUTE GENETIC YOUTH PERCENTAGE
  generic_youth <- n_distinct(subset(age_groups, faixa %in% c("Jovens", "Youth"))[, "people_id"])/n_distinct(age_groups$people_id)
  
  out <- merge(out, cadeias_provinces, by = "cadeias_province", all.x = TRUE)
  out <- merge(out, provincias, by = "provincia", all.x = TRUE)
  out <- merge(out, cadeias, by = "Description", all.x = TRUE)
  out <- out %>% mutate(youth_pct = case_when(
    is.na(youth_pct) & !is.na(jovens_provincias) ~ jovens_provincias,
    is.na(youth_pct) & is.na(jovens_provincias) ~ jovens_cadeias,
    TRUE ~ youth_pct
  ))
  out$youth_pct[is.na(out$youth_pct)] <- generic_youth
  
  out <- out %>% mutate(
    MW = Total,
    W = `F`,
    YTH = round(Total*youth_pct,0),
    WYTH = round(W*youth_pct,0),
    `Women (%)` = round(W/Total*100,2),
    `Youth (%)` = round(youth_pct*100,2),
  )  %>% arrange(provincia, -Total)
  
  total_geral <- out %>% group_by(Description) %>% summarise(MW = round(sum(MW),0),
                                                             W = round(sum(W),0),
                                                             YTH = round(sum(YTH),0),
                                                             WYTH = round(sum(WYTH),0),
                                                             `Women (%)` = round(sum(W)/sum(MW)*100,2),
                                                             `Youth (%)` = round(YTH/MW*100,2)) %>% arrange(-MW)
  total_geral$Description <- sub('Total','TOTAL',total_geral$Description)
  
  out <- out  %>% mutate(Description = case_when(
    Description =='Total' ~ provincia,
    TRUE ~ Description
  ))                                           
  
  final_output <- dplyr::bind_rows(out, total_geral)
  final_output$order <- rownames(final_output)
  final_output$order <- as.numeric(final_output$order)
  
  final_output$cadeias_province <- ifelse(is.na(final_output$cadeias_province), final_output$Description, final_output$cadeias_province)
  
  pwd <- data %>% dplyr::filter(disa != "1A") %>% 
    group_by(provincia, Description, sexo) %>%
    summarise(beneficiaries = n_distinct(people_id, na.rm = TRUE), .groups = 'drop') %>%
    pivot_wider(names_from = sexo, values_from = beneficiaries) %>%
    group_by(provincia) %>%
    group_modify(~ .x %>% adorn_totals(where = "row")) %>%
    ungroup %>%
    split_columns(columns = 1)
  
  pwd$provincia[pwd$provincia== ""] <- NA
  pwd <- pwd %>% fill(provincia, .direction = "down") %>% adorn_totals("col") %>% 
    dplyr::filter(Total > 0)
  pwd$cadeias_province <- paste0(pwd$provincia, pwd$Description)
  age_groups <- data %>% dplyr::filter(ano > 0 & !is.na(ano))
  age_groups$cadeias_province <- paste0(age_groups$provincia, age_groups$Description)
  # YpwdH PROVINCE AND VALUE CHAIN
  cadeias_provinces <- age_groups %>% group_by(cadeias_province, faixa) %>%
    summarize(beneficiaries = n_distinct(people_id)) %>% 
    spread(faixa, -cadeias_province) %>% adorn_totals("col") %>% mutate(ypwdh_pct = Jovens/Total) %>% select(cadeias_province, ypwdh_pct)
  # YpwdH PER VALUE CHAIN
  cadeias <- age_groups %>% group_by(Description, faixa) %>% 
    summarise(beneficiaries = n_distinct(people_id)) %>% spread(faixa, - Description) %>% adorn_totals("col") %>%
    mutate(jovens_cadeias = Jovens/Total) %>% select(Description, jovens_cadeias)
  ## YpwdH PER PROVINCE
  provincias <- age_groups %>% group_by(provincia, faixa) %>% 
    summarise(beneficiaries = n_distinct(people_id)) %>% spread(faixa, - provincia) %>% adorn_totals("col") %>%
    mutate(jovens_provincias = Jovens/Total) %>% select(provincia, jovens_provincias) %>% dplyr::filter(!is.na(provincia))
  ## COMPUTE GENETIC YpwdH PERCENTAGE
  generic_ypwdh <- n_distinct(subset(age_groups, faixa %in% c("Jovens", "Ypwdh"))[, "people_id"])/n_distinct(age_groups$distinct_name)
  
  pwd <- merge(pwd, cadeias_provinces, by = "cadeias_province", all.x = TRUE)
  pwd <- merge(pwd, provincias, by = "provincia", all.x = TRUE)
  pwd <- merge(pwd, cadeias, by = "Description", all.x = TRUE)
  pwd <- pwd %>% mutate(ypwdh_pct = case_when(
    is.na(ypwdh_pct) & !is.na(jovens_provincias) ~ jovens_provincias,
    is.na(ypwdh_pct) & is.na(jovens_provincias) ~ jovens_cadeias,
    TRUE ~ ypwdh_pct
  ))
  pwd$ypwdh_pct[is.na(pwd$ypwdh_pct)] <- generic_ypwdh
  
  pwd <- pwd %>% mutate(
    MW = Total,
    W = `F`,
    YTH = round(Total*ypwdh_pct,0),
    WYTH = round(W*ypwdh_pct,0),
    `Women (%)` = round(W/Total*100,2),
    `Ypwdh (%)` = round(ypwdh_pct*100,2),
  )  %>% arrange(provincia, -Total)
  
  total_pwd <- pwd %>% group_by(Description) %>% summarise(MW = round(sum(MW),0),
                                                           W = round(sum(W),0),
                                                           YTH = round(sum(YTH),0),
                                                           WYTH = round(sum(WYTH),0),
                                                           `Women (%)` = round(sum(W)/sum(MW)*100,2),
                                                           `Ypwdh (%)` = round(YTH/MW*100,2)) %>% arrange(-MW)
  total_pwd$Description <- sub('Total','TOTAL',total_pwd$Description)
  
  pwd <- pwd  %>% mutate(Description = case_when(
    Description =='Total' ~ provincia,
    TRUE ~ Description
  ))
  
  final_pwd <- dplyr::bind_rows(pwd, total_pwd) %>% dplyr::select(descricao = Description, cadeias_province, PwD = MW, WwD = W)
  final_pwd$cadeias_province <- ifelse(is.na(final_pwd$cadeias_province), final_pwd$descricao, final_pwd$cadeias_province)
  final_output <- merge(final_output, final_pwd, by = "cadeias_province", all.x = TRUE)
  
  quantidade_cadeia_prov <- data %>% group_by(provincia, Description) %>% summarise(Number = sum(quantidade)) %>% 
    adorn_totals("row", name = "TOTAL")
  quantidade_provincial <- data %>% group_by(provincia) %>% summarise(Number = sum(quantidade)) %>% 
    add_column(Description = "TOTAL")
  quantidade_cadeias <- data %>% group_by(Description) %>% summarise(Number = sum(quantidade)) %>% 
    mutate(Descricao = Description)
  
  quantidade_all <- dplyr::bind_rows(quantidade_cadeia_prov, quantidade_provincial, quantidade_cadeias)
  quantidade_all <-   quantidade_all %>% mutate(provincia = ifelse(is.na(provincia), Description, provincia)) %>% 
    mutate(cadeias_province = paste0(provincia, Description)) %>% 
    mutate(Description = ifelse(Description == "TOTAL", provincia,
                                ifelse(Description == '-', provincia, Description))) %>% 
    select(provincia, Description, Number)
  final_output <- final_output %>% mutate(provincia = ifelse(is.na(provincia), Description, provincia))
  
  final_output <- final_output %>% 
    dplyr::full_join(quantidade_all, by = c("provincia", "Description"))
  
  final_output$`PwD (%)` = round(final_output$PwD/final_output$MW*100,2)
  final_output <- final_output %>% arrange(order)
  
  final_output$cadeias_province[is.na(final_output$cadeias_province)] <- final_output$Description
  final_output <- final_output%>% distinct(cadeias_province, .keep_all = TRUE) %>% arrange(order) %>% 
    dplyr::select(Description, Number, MW, W, YTH, WYTH, PwD, WwD, `Women (%)`, `Youth (%)`, `PwD (%)`)%>%
    mutate_if(is.numeric , replace_na, replace = 0)
  
  return(final_output)
}

{

  
  
  
  box_title_js <- '
  Shiny.addCustomMessageHandler("box_title", function(title) {
  if(title.includes("mpg")){
    colour = "red"
  } else {
    colour = "blue"
  }
    $("#box_plot h3").html(title)
    $("#box_plot .card-body").css("background-color", colour)
  });
'
  
  
  initComplete = JS(
    "function(settings, json) {",
    "$(this.api().table().header()).css({'background-color': '#228B22', 'color': '#fff'});",
    "$(this.api().table().body()).css({'font-weight': 'normal'});",
    "}"
  ) ########### FORMATING DATATABLES TO INSTUTIONAL TABLES AND COLORS
} ############ FUNCTIONS
{
  period_started <- as.Date('2023-04-01', origin = '1970-01-01')
  period_ended <- as.Date('2023-06-30', origin = '1970-01-01')
  quarter <- quarter(period_ended)
  exchange_rate <- 64.52
  e_rate = exchange_rate
  forex_e_rate = 63.20
  options(scipen=100000000000000)
  title <- tags$a(href='https://finance.far.org',tags$img(src="PROCAVA_LOGO.png", height = '97.5', width = '220'), '', target="_blank")
}  ########## COLNAMES
# {
#   mozpostos_read <- st_read("moz_admbnda_adm3_ine_20190607.shp", quiet = TRUE)
#   mozpostos_read <- ms_simplify(mozpostos_read, keep = 0.01, keep_shapes = TRUE)
#   Distritos <- ms_dissolve(mozpostos_read, field = c("ADM2_PT"))
#   mozpostos_geom <- st_geometry(mozpostos_read)
#   limitesdistritais_geom <- st_geometry(Distritos )
#   mozpostos_wgs84 <- mozpostos_read %>%
#     st_buffer(0) %>% 
#     st_transform(crs = 4326) %>% 
#     mutate(id = 1:nrow(.)) 
#   limitesdistritais_wgs84 <- Distritos %>%
#     st_buffer(0) %>% 
#     st_transform(crs = 4326) %>% 
#     mutate(id = 1:nrow(.)) 
#   Breaks <- c(0, 1 ,25, 50, 75, 100, 150, 200, 250, Inf)
#   Labels <-c("0", "1-25" ,"25-50", "50-75", "75-100", "100-150", "150-200", "200-250","250+")
#   moz_districts <- read_fst("moz_districts.fst", columns = "district_name")
# }  ###################  GIS LAYERS


administrative <- data.frame(admin_prov = c("Maputo Cidade", "Maputo Cidade", "Maputo Cidade"),admin_distrito = c("KaMavota", "KaMavota", "KaMavota"),localidade = c("Mahotas", "3 de Fevereiro", "Laulane"))

{
  coltypes_entrega_sementes = c('text', 'text', 'numeric', 'date', 'text', 'numeric', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text')
  coltypes_entrega_agroquimicos = c('text', 'text', 'numeric', 'date', 'text', 'numeric', 'text', 'numeric', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')

  coltypes_entrega_equipamento_normal =c('text', 'date', 'numeric', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text', 'text', 'text', 'numeric', 'numeric')
  coltypes_entrega_equipamento_unnormal = c('text', 'text', 'numeric', 'date', 'numeric', 'text', 'numeric', 'numeric', 'text', 'text', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
  
  coltypes_vacinacao_newcastle = c('text', 'text', 'date', 'numeric', 'numeric', 'text', 'numeric', 'text', 'text', 'text', 'numeric', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
  coltypes_vacinacao_obrigatoria =c('text', 'text', 'numeric', 'date', 'numeric', 'text', 'numeric', 'text', 'text', 'numeric', 'text', 'text', 'text', 'numeric', 'numeric', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
  
  coltypes_treinamento_beneficiarios =c('text', 'text', 'numeric', 'text', 'text', 'numeric', 'text', 'text', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text')
  coltypes_treinamento_tecnicos =c('text', 'text', 'text', 'numeric', 'text', 'text', 'text', 'text', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text')

  coltypes_deliveries_beneficiaries = c('text', 'text', 'numeric', 'text', 'text', 'numeric', 'text', 'text', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'numeric', 'text', 'text', 'text', 'numeric', 'text', 'text')
  
  coltypes_deliveries_agrochemicals =   c('text', 'text', 'numeric', 'date', 'text', 'numeric', 'text', 'numeric', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
  coltypes_deliveries_msme =   c('text', 'text', 'text', 'numeric', 'text', 'numeric', 'numeric', 'numeric', 'text', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
  coltypes_deliveries_events =  c('text', 'date', 'numeric', 'text', 'numeric', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text')
  # coltypes_entrega_animais =   c('text', 'text', 'text', 'numeric', 'numeric', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text')
  
  coltypes_vendas_feiras = c('text', 'numeric', 'date', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'text', 'numeric', 'numeric', 'numeric', 'text')
  coltypes_training_certificates = c('text', 'text', 'text', 'text', 'text', 'numeric', 'date')
  coltypes_training_actions = c('text', 'text', 'text', 'date', 'text', 'numeric', 'date')

  coltypes_treinamento_beneficiarios =c('text', 'text', 'numeric', 'text', 'text', 'numeric', 'text', 'text', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text')
  coltypes_treinamento_tecnicos =c('text', 'text', 'text', 'numeric', 'text', 'text', 'text', 'text', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text')
  coltypes_training_sessions = c('text', 'numeric', 'date', 'date', 'text', 'numeric', 'numeric', 'numeric', 'text', 'numeric', 'text', 'text', 'text', 'text')
  coltypes_training_venues = c('text', 'text', 'date', 'numeric', 'text', 'numeric', 'numeric')
  coltypes_traning_topics = c('text', 'text', 'text', 'numeric', 'text', 'numeric', 'date')

  coltypes_entrega_animais = c('text', 'text', 'text', 'numeric', 'numeric', 'numeric', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 'numeric', 'text', 'text')
  
  unwanted_array = list(    'Š'='S', 'š'='s', 'Ž'='Z', 'ž'='z', 'À'='A', 'Á'='A', 'Â'='A', 'Ã'='A', 'Ä'='A', 'Å'='A', 'Æ'='A', 'Ç'='C', 'È'='E', 'É'='E',
                            'Ê'='E', 'Ë'='E', 'Ì'='I', 'Í'='I', 'Î'='I', 'Ï'='I', 'Ñ'='N', 'Ò'='O', 'Ó'='O', 'Ô'='O', 'Õ'='O', 'Ö'='O', 'Ø'='O', 'Ù'='U',
                            'Ú'='U', 'Û'='U', 'Ü'='U', 'Ý'='Y', 'Þ'='B', 'ß'='Ss', 'à'='a', 'á'='a', 'â'='a', 'ã'='a', 'ä'='a', 'å'='a', 'æ'='a', 'ç'='c',
                            'è'='e', 'é'='e', 'ê'='e', 'ë'='e', 'ì'='i', 'í'='i', 'î'='i', 'ï'='i', 'ð'='o', 'ñ'='n', 'ò'='o', 'ó'='o', 'ô'='o', 'õ'='o',
                            'ö'='o', 'ø'='o', 'ù'='u', 'ú'='u', 'û'='u', 'ý'='y', 'ý'='y', 'þ'='b', 'ÿ'='y' )
  }  ########## COLTYPES
{
  variables = c('paletes', 'caixasdehorticolas', 'sacoshermeticos', 'embalagensdefeijaomil', 
                          'embalagensdemandiocamil', 'sacosdebatatamil', 'sacosdecebolamil', 'refractometro', 
                          'tractormec', 'charruamecanizadamec', 'gradede16discosmec', 'semeadoramecanizadamec', 
                          'motocultivadoramec', 'debulhadoramec', 'semeadoradegraosmec', 'semeadoradehorticolasmec', 
                          'semeadoradegergelimmec', 'sachadoramanualmec', 'cadernoderegistosmec', 'bicicletapcc',
                          'mochilapcc', 'pulverizadordorsalpcc', 'fatomacacopcc', 'oculosdeproteccaopcc', 'pardeluvaspcc',
                          'botaspcc', 'capadechuvapcc', 'mascaraprotectorapcc', 'seringasdescartaveis20mlpcc', 'luvascirurgicapcc',
                          'agulhassubcutaneaspcc', 'agulhasintramuscularespcc', 'agulhasendovenosaspcc', 'agulhasdescartaveispcc', 
                          'aneisdecastracaocaprinapcc', 'burdizopcc', 'alicateelastradorpcc', 'laminasdebisturipcc', 'termometropcc',
                          'cadernoderegistospcc', 'medicamentosveterinariospcc', 'pluviometropcc', 'bicicletavco', 'colmanvco',
                          'contagotasvco', 'megafonevco', 'mochilavco', 'camisetevco', 'bonevco', 'cadernoderegistosvco', 
                          'paletes', 'caixasdehorticolas', 'sacoshermeticos', 'embalagensdefeijaomil', 'embalagensdemandiocamil', 
                          'sacosdebatatamil', 'sacosdecebolamil', 'tractor', 'charruamecanizada', 'gradede16discos', 
                          'semeadoramecanizada', 'motocultivadora', 'debulhadora', 'semeadoradegraos', 'semeadoradehorticolas',
                          'semeadoradegergelim', 'sachadoramanual', 'cadernoderegistos', 'balancamanual25kgpac', 'bicicletapac',
                          'capadechuvapac', 'carrinhopac', 'chapeupac', 'cordapac', 'factomacacopac', 'fitametricapac', 'galochaspac',
                          'kitinsumospac', 'pardeluvaspac', 'oculosprotectorespac', 'pulverizadordorsalpac', 'respiradorpac', 
                          'assistenciatecnicapac', 'tamborde200lpac', 'balancaindustrial100kgpac', 'mascaraprotectorapac', 
                          'aventalpac', 'cadernoderegistospac', 'pluviometropac', 'megafonepac', 'bovinomachota', 'bovinofemeata', 
                          'carrocadetraccaoanimalta', 'charruadetraccaoanimalta', 'acessoriosdetata', 'medicamentosveterinariosta', 
                          'cangasecangalhosta', 'pulverizadordorsalta', 'luvasta', 'fatomacacota', 'respiradoresta', 'oculosprotectoresta',
                          'chapeuta', 'cadernoderegistosta', 'raspador', 'lixadeiramoinho', 'manjedoratriturador', 'bandejasdeseca', 
                          'kitdeferramentas', 'cortadorabatata', 'cortadorabolachas', 'fritadeirabatata', 'cortadoraforragem', 
                          'moinhodemandioca', 'trituradomandioca', 'peneiramanual', 'prensamanualdupla', 'prensa1barril', 
                          'torradeiraemmassa', 'balancaelectronica', 'moinhodealimentos', 'processadormanualdesumo', 
                          'luvasdescartaveistransparentes', 'descascadordelegumes', 'garrafasdevidro', 'garrafasdeplastico', 
                          'sacodeplasticoparalegumes', 'garrafasplasticas350ml', 'garrafasplasticascomtampa200ml', 'pratodeisopor500ml', 
                          'baciasacoinox50l', 'baciasacoinox20l', 'baciasacoinox15l', 'panelasacoinox10l', 'tachosacoinox10l', 
                          'escumadeirametalicaemacoinox', 'colherdemadeira', 'baseparacortarlegumes', 'fogaopoupacarvao',
                          'funilemmaterialpvc', 'canecade500ml', 'cadeiraplastica', 'acoinoxidavel', 'cacasdeacoinox',
                          'copomedidordevidromultiusos', 'rolodepeliculaplastica', 'pratodeacoinoxidavel', 
                          'embalagemde100tampas', 'aventaldecozinha', 'espremedordefrutamanual', 'jarradevidro2l',
                          'tacasplasticas10l', 'tacasplasticas10l', 'coberturademesaaprovadeagua', 'rolodeguardanapos',
                          'mesadobravelcombase', 'caixotedolixo', 'balancaeletronicadeprecisao', 'pratosdemetal', 
                          'coposdealuminio300ml', 'moedordecarnemanual', 'enchedordecarne', 'ganchometalicoparasecarcarne',
                          'caixademadeira', 'secadorsolardelegumes', 'maquinadeselar', 'formadeboloemaluminio', 'tabuleiroredondo',
                          'gazebos', 'frigideiradealuminio', 'conchafundadeacoinoxidavel', 'basesdeboloemaluminio', 
                          'cartuchoempapelcaqui', 'colherdesopa', 'caixasdetransporte')
  
  labels = c('Paletes de armazenamento', 'Caixas de Hortícolas', 'Sacos Herméticos de 50 kg', 'Embalagens de Feijão (milhares)', 
                  'Embalagens de Mandioca (milhares)', 'Sacos de Batata (milhares)', 'Sacos de Cebola (milhares)',
                  'Refractómetro Portátil', 'Tractor agrícola', 'Charrua Mecanizada', 'Grade de 16 Discos', 'Semeadora Mecanizada', 
                  'Motocultivadora', 'Debulhadora', 'Semeadora de Grãos', 'Semeadora de Hortícolas', 'Semeadora de Gergelim', 
                  'Sachadora Manual', 'Caderno de registos', 'Bicicleta', 'Mochila', 'Pulverizador Dorsal', 'Fato Macaco', 
                  'Óculos Protectores', 'Par de Luvas', 'Botas', 'Capa de Chuva', 'Máscara Protectora', 'Seringas Descartáveis de 20 ml',
                  'Luvas cirúrgicas', 'Agulhas subcutâneas', 'Agulhas intramusculares', 'Agulhas endovenosas', 'Agulhas descartáveis', 
                  'Anéis de castração', 'Burdizo', 'Alicate elastrador', 'Lâminas de Bisturi', 'Termómetro', 'Caderno de registos', 
                  'Medicamentos Veterinários', 'Pluviómetro', 'Bicicleta', 'Colman', 'Conta-gotas', 'Megafone', 'Mochila', 'Camisete',
                  'Boné', 'Caderno de registos', 'Paletes', 'Caixas de Hortícolas', 'Sacos Herméticos de 50 kg', 
                  'Embalagens de Feijão (milhares)', 'Embalagens de Mandioca (milhares)', 'Sacos de Batata (milhares)', 
                  'Sacos de Cebola (milhares)', 'Tractor', 'Charrua Mecanizada', 'Grade de 16 Discos', 'Semeadora Mecanizada', 
                  'Motocultivadora', 'Debulhadora', 'Semeadora de Grãos', 'Semeadora de Hortícolas', 'Semeadora de Gergelim', 
                  'Sachadora Manual', 'Caderno de registos', 'Balança Manual de 25 kg', 'Bicicleta', 'Capa de Chuva',
                  'Carrinho', 'Chapéu', 'Corda', 'Fato Macaco', 'Fita Métrica', 'Galochas (Botas)', 'Kit Insumos', 
                  'Par de Luvas', 'Óculos Protectores', 'Pulverizador Dorsal', 'Respirador', 'Assistência Técnica', 
                  'Tambor de 200 Litros', 'Balança Industrial de 100 kg', 'Máscara Protectora', 'Avental',
                  'Caderno de registos', 'Pluviómetro', 'Megafone', 'Boi de tracção', 'Vaca de tracção e reprodução',
                  'Carroça de tracção animal', 'Charrua de tracção animal', 'Acessórios de tracção animal', 'Medicamentos veterinários',
                  'Cangas e cangalhos', 'Pulverizador dorsal de 16 litros', 'Luvas', 'Fato Macaco', 'Respiradores', 'Óculos protectores', 
                  'Chapéu', 'Caderno de registos', 'Raspador ou esfaqueador motorizado de raízes de mandioca', 
                  'Lixadeira ou Moinho (Desintegrador)', 'Manjedoura ou torrador para guardar a massa ou farinha', 
                  'Bandejas para a secagem do chips de mandioca', 'Kits de ferramentas', 'Máquina de corte de batatas fritas', 
                  'Máquina de bolachas de batata operada manualmente', 'Máquina de fritar batatas', 'Máquina de corte de forragem',
                  'Trituradora ou moinho de mandioca', 'Trituradora para massa de mandioca prensada/ massa', 
                  'Peneira manual para massa ou massa prensada', 'Prensa manual, com coluna dupla', 'Prensa manual de um barril',
                  'Torrefador em massa', 'Balança electrónica digital', 'Moinho de alimentos', 'Processador manual de sumo', 
                  'Luvas descartáveis transparentes', 'Descascador de legumes', 'Garrafas de vidro', 'Garrafas de plástico', 
                  'Saco de plástico para legumes', 'Garrafas plásticas de 350 ml', 'Garrafas plásticas com tampa de 200 ml',
                  'Prato de isopor de 500 ml', 'Bacias aço-inox de 50 litros', 'Bacias aço-inox de 20 litros',
                  'Bacias aço-inox de 15 litros', 'Panelas aço-inox de 10 litros', 'Tachos aço-inox de 10 litros',
                  'Escumadeira metálica em aço-inox', 'Colher de madeira', 'Base para cortar legumes', 'Fogão poupa carvão', 
                  'Funil em material PVC', 'Caneca de 500 ml', 'Cadeira plástica', 'Aço inoxidável', 'Cacas de aço-inox', 
                  'Copo medidor de vidro multiusos', 'Rolo de película plástica', 'Prato de aço inoxidável', 'Embalagem de 100 tampas', 
                  'Avental de cozinha', 'Espremedor de fruta manual', 'Jarra de vidro, 2 litros', 'Taças plásticas, 10 litros',
                  'Taças plásticas, 10 litros', 'Cobertura de mesa à prova de água', 'Rolo de guardanapos', 'Mesa dobrável com base',
                  'Caixote do lixo', 'Balança eletrónica de precisão', 'Pratos de metal', 'Copos de alumínio, 300 ml', 
                  'Moedor de carne manual', 'Enchedor de carne', 'Gancho metálico para secar carne', 'Caixa de madeira',
                  'Secador solar de legumes', 'Máquina de selar', 'Forma de bolo em alumínio', 'Tabuleiro redondo', 'Gazebos', 
                  'Frigideira de alumínio', 'Concha funda de aço-inoxidável', 'Bases de bolo em alumínio', 'Cartucho em papel caqui',
                  'Colher de sopa', 'Caixas de transporte de itens culinários')
  
  dicionario <- data.frame(variavel = variables, label = labels)
css <- "
::-moz-placeholder { /* Firefox 19+ */
  color: pink;
}
:-ms-input-placeholder { /* IE 10+ */
  color: pink;
}
:-moz-placeholder { /* Firefox 18- */
  color: pink;
}"
}  ### CONSTANTS
{
data(mtcars)
}  ########## DATASETS

shinyApp(options = list(launch.browser = TRUE),
  ui = dashboardPage(
    header = dashboardHeader(
      title =  title
    ),
    sidebar = dashboardSidebar(collapsed = TRUE, 
                               sidebarMenu(
                                 menuItem("ALCANCE GLOBAL", startExpanded = FALSE, icon = icon("chart-line"),
                                          menuSubItem("Detalhes", tabName = "farfp_outreach")
                                 ),
                                 menuItem("CAPACITAÇÕES", startExpanded = FALSE, icon = icon("chart-line"),
                                          menuSubItem("Técnicos", tabName = "technicians_trainings"),
                                          menuSubItem("Beneficiários", tabName = "beneficiaries_trainings")
                                 ),
                                 menuItem("VACINAS & INSUMOS", startExpanded = FALSE, icon = icon("chart-line"),
                                          menuSubItem("Insumos", tabName = "inputs_deliveries"),
                                          menuSubItem("Vacinações", tabName = "vaccinations"),
                                          menuSubItem("Equipamento", tabName = "equipment_deliveries")
                                 ),
                                 menuItemOutput('server_menu')
                                 
                               )),
    body = dashboardBody( tags$head(tags$style("#test .modal-dialog {width: fit-content !important;}")),
                          
      tabItems(
        tabItem("farfp_outreach",
                fluidRow(
                  valueBoxOutput("beneficiarios_registados", width=3),
                  valueBoxOutput("tecnicos_envolvidos", width=3),
                  valueBoxOutput("organizacoes_produtores", width=3),
                  valueBoxOutput("agentes_comunitarios",  width=3),

                box(title = "OS 10 MAIORES CONTRIBUINTES", closable = TRUE, maximizable = TRUE, width = 6, 
                    height = "730px",  solidHeader = FALSE,  background = NULL, status = "success",  collapsible = TRUE,
                    sidebar = boxSidebar(id = "mycardsidebasrs", width = 25),
                  plotOutput("top_beneficiaries_plot")),
                
                
                box(title = "COBERTURA GEOGRÁFICA", closable = TRUE, width = 6, height = "730px", collapsible = TRUE,
                    collapsed = FALSE, maximizable = TRUE, solidHeader = FALSE,  background = NULL, status = "success",
                    tmapOutput("cobertura_geografica")),

                box(collapsed = TRUE, maximizable = TRUE, tags$script(box_title_js), title = "ANÁLISES", width = 12,
                    DT::DTOutput("resumo_outreach", width = "100%", height = "auto", fill = TRUE))
                )),
        
        tabItem("beneficiaries_trainings", actionButton("goWidgetss", "Go to Widgets tabsss"))
      )
    ),
    

    controlbar = dashboardControlbar(tags$head(tags$style(HTML(css))),
                                       column(12, actionButton("carregar_dados", "Enviar dados!", status = "success", outline = TRUE, flat = FALSE, size = "lg", icon = icon("laptop")), align = "center" , style = "margin-bottom: 10px;" , style = "margin-top: 10px;"),
                                       column(12, actionButton("obter_fichas", "Baixar ficha!", status = "primary", outline = TRUE, flat = FALSE, size = "lg", icon = icon("download")), align = "center" , style = "margin-bottom: 10px;" , style = "margin-top: 10px;")
      
      ),

    footer = bs4DashFooter(left = a(href = "www.farfp.com", target = "_blank", "#FAR-FP"), right = paste0("Actualização: ", format(Sys.Date(), "%b"), " ", format(Sys.Date(), "%Y")))),
  
  server = function(input, output, session) {
    
    output$provinces_admin <- renderUI({
      selectizeInput("provincias_admin", "PROVÍNCIA", choices = c("Todas", unique(administrative$admin_prov)), selected = "Todas")
    })
    
    output$districts_admin <- renderUI({
      selectizeInput("distritos_admin", "Distrito", choices = c(administrative$admin_distrito[administrative$admin_prov == input$provincias_admin], "Todos"), selected="Todos")
    })
    
    output$localities_admin <- renderUI({
      selectizeInput("localidades_admin", "Localidade", choices = c(administrative$localidade[administrative$admin_distrito == input$distritos_admin], "Todas"),  selected = "Todas")
    })
    

    
    District_admin <- sort(unique(as.character(administrative$admin_prov)))
    Province_admin <- sort(unique(as.character(administrative$admin_distrito)))
    Locality_admin <- sort(unique((administrative$localidade)))
    
    
    heirarchy<-c("All", Province_admin)
    observe({
      hei1<-input$Province_admin
      hei2<-input$District_admin
      hei3<-input$Locality_admin
      
      choice1<-c("NONE",setdiff(heirarchy,c(hei2,hei3)))
      choice2<-c("NONE",setdiff(heirarchy,c(hei1,hei3)))
      choice3<-c("NONE",setdiff(heirarchy,c(hei1,hei2)))
      
      updateSelectInput(session,"heir1",choices=choice1,selected=hei1)
      updateSelectInput(session,"heir2",choices=choice2,selected=hei2)
      updateSelectInput(session,"heir3",choices=choice3,selected=hei3)
      
    })

    observeEvent(input$carregar_dados, {
      showModal(modalDialog(title = p(HTML("<b><font color='#228B22'>ALOJAMENTO DE DADOS</font><b/>")),
        
        fluidRow(
          column(
            width=3,
            selectizeInput("lista_carregada", label = NULL, choices = c("Selecione o tipo da dados", "Entrega de Animais" = "entrega_animais", 
                                                                "Entrega de Pacotes de Insumos" = "entrega_insumos",
                                                                "Entrega de Semente" = "entrega_semente",
                                                                "Entrega de Equipamento" = "entrega_equipamento",
                                                                "Entrega de Agroquímicos" = "entrega_agroquimicos",
                                                                "Vacinação contra a Newcastle" = "vacinacao_newcastle",
                                                                "Vacinação Obrigatória" = "vacinacao_obrigatoria",
                                                                "Treinamento de Beneficiários" = "treinamento_beneficiarios",
                                                                "Treinamento de Técnicos" = "treinamento_tecnicos"))),
          
          column(width= 3, 
                 conditionalPanel("input.lista_carregada == 'entrega_animais'", fileInput("entrega_animais_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'entrega_insumos'", fileInput("entrega_insumos_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'entrega_semente'", fileInput("entrega_semente_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'entrega_equipamento'", fileInput("entrega_equipamento_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'entrega_agroquimicos'", fileInput("entrega_agroquimicos_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_newcastle'", fileInput("vacinacao_newcastle_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_obrigatoria'", fileInput("vacinacao_obrigatoria_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_tecnicos'", fileInput("treinamento_tecnicos_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_beneficiarios'", fileInput("treinamento_beneficiarios_xlsx", NULL, placeholder = "Excel (Digitada)", accept = c(".xls", ".xlsx")))),
          
          column(width= 3, 
                 conditionalPanel("input.lista_carregada == 'entrega_animais'", fileInput("entrega_animais_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_insumos'", fileInput("entrega_insumos_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_semente'", fileInput("entrega_semente_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_equipamento'", fileInput("entrega_equipamento_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_agroquimicos'", fileInput("entrega_agroquimicos_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_newcastle'", fileInput("vacinacao_newcastle_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_obrigatoria'", fileInput("vacinacao_obrigatoria_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_tecnicos'", fileInput("treinamento_tecnicos_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_beneficiarios'", fileInput("treinamento_beneficiarios_pdf", NULL, placeholder = "PDF, Imagem (assinda)", accept = c(".pdf", ".png", ".jpeg", ".jpg")))),
          
          column(width= 3, 
                 conditionalPanel("input.lista_carregada == 'entrega_animais'", fileInput("entrega_animais_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_insumos'", fileInput("entrega_insumos_foto", NULL, placeholder = "Fotografia", accept = c( ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_semente'", fileInput("entrega_semente_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_equipamento'", fileInput("entrega_equipamento_foto", NULL, placeholder = "Fotografia", accept = c( ".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'entrega_agroquimicos'", fileInput("entrega_agroquimicos_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_newcastle'", fileInput("vacinacao_newcastle_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'vacinacao_obrigatoria'", fileInput("vacinacao_obrigatoria_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_tecnicos'", fileInput("treinamento_tecnicos_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))),
                 conditionalPanel("input.lista_carregada == 'treinamento_beneficiarios'", fileInput("treinamento_beneficiarios_foto", NULL, placeholder = "Fotografia", accept = c(".png", ".jpeg", ".jpg"))))
          
          ),
        
        fluidRow(
          conditionalPanel("input.lista_carregada == 'entrega_animais'", DT::DTOutput("entrega_animais_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'entrega_insumos'", DT::DTOutput("entrega_insumos_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'entrega_semente'", DT::DTOutput("entrega_semente_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'entrega_equipamento'", DT::DTOutput("entrega_equipamento_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'entrega_agroquimicos'", DT::DTOutput("entrega_agroquimicos_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'vacinacao_newcastle'", DT::DTOutput("vacinacao_newcastle_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'vacinacao_obrigatoria'", DT::DTOutput("vacinacao_obrigatoria_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'treinamento_tecnicos'", DT::DTOutput("treinamento_tecnicos_novo", width = "100%", height = "auto", fill = TRUE)),
          conditionalPanel("input.lista_carregada == 'treinamento_beneficiarios'", DT::DTOutput("treinamento_beneficiarios_novo", width = "100%", height = "auto", fill = TRUE))
        ),
        easyClose = FALSE,  size = "xl", footer = tagList(modalButton("Cancel"),
                                                          conditionalPanel("input.lista_carregada == 'entrega_insumos'", actionButton("salvar_entrega_insumos", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'entrega_animais'", actionButton("salvar_entrega_animais", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'entrega_semente'", actionButton("salvar_entrega_semente", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'entrega_equipamento'", actionButton("salvar_entrega_equipamento", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'entrega_agroquimicos'", actionButton("salvar_entrega_agroquimicos", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'vacinacao_newcastle'", actionButton("salvar_vacinacao_newcastle", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'vacinacao_obrigatoria'", actionButton("salvar_vacinacao_obrigatoria", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'treinamento_tecnicos'", actionButton("salvar_treinamento_tecnicos", "Submeter", status = "success")),
                                                          conditionalPanel("input.lista_carregada == 'treinamento_beneficiarios'", actionButton("salvar_treinamento_beneficiarios", "Submeter", status = "success"))
                                                          ), fade = TRUE))

    })
    
    output$entrega_animais_novo <- renderDT({
      req(input$entrega_animais_xlsx)
      
      dados_brutos <- read_excel(designacao_ficheiro, sheet = "livestock_deliveries", col_types = colnames_entrega_animais) %>% 
        dplyr::filter(!is.na(apelido) & !is.na(people_admin_id)& !is.na(admin_id) & !is.na(delivery_date))%>%
        mutate(delivery_date = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, Ano = ano, Localidade = localidade,  Povoado = povoado, `Espécie` = species,
                      `Raça` = breed,  Categoria = category, `Geração` = generation, `Animais entregues` = quantity,
                      `Entrega` =delivery_date) %>% dplyr::filter(Nome != "TESTE")
      
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
      })
    
    output$entrega_animais_novo <- renderDT({
      req(input$entrega_animais_xlsx)
      
      dados_brutos <- read_excel(designacao_ficheiro, sheet = "livestock_deliveries", col_types =coltypes_entrega_animais) %>% 
        dplyr::filter(!is.na(apelido) & !is.na(people_admin_id)& !is.na(admin_id) & !is.na(delivery_date))%>%
        mutate(delivery_date = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, Ano = ano, Localidade = localidade,  Povoado = povoado, `Espécie` = species,
                      `Raça` = breed,  Categoria = category, `Geração` = generation, `Animais entregues` = quantity,
                      `Entrega` =delivery_date) %>% dplyr::filter(Nome != "TESTE") 
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
      })
    
    output$resumo_outreach <- renderDT({
      dataset <- dbGetQuery(far_pool, paste0("SELECT * FROM vistas.outreach WHERE intervencao = 'Vacinação Contra a Newcastle'"))
      dados_brutos <- beneficiary_counting_quantidade(dataset)
      # dados_brutos <- read_excel(designacao_ficheiro, sheet = "livestock_deliveries", col_types =coltypes_entrega_animais) %>% 
      #   dplyr::filter(!is.na(apelido) & !is.na(people_admin_id)& !is.na(admin_id) & !is.na(delivery_date))%>%
      #   mutate(delivery_date = format(delivery_date, "%d/%m/%Y")) %>% 
      #   dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, Ano = ano, Localidade = localidade,  Povoado = povoado, `Espécie` = species,
      #                 `Raça` = breed,  Categoria = category, `Geração` = generation, `Animais entregues` = quantity,
      #                 `Entrega` =delivery_date) %>% dplyr::filter(Nome != "TESTE") 
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    
    
    output$entrega_insumos_novo <- renderDT({
      req(input$entrega_insumos_xlsx)
      sementes <- read_excel(input$entrega_insumos_xlsx$datapath, sheet = "seed_deliveries", col_types =   coltypes_entrega_sementes) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id))
      agroquimicos <- read_excel(input$entrega_insumos_xlsx$datapath, sheet = "agrochemicals", col_types =   coltypes_entrega_agroquimicos) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        pivot_longer(cols = c(11:ncol(read_excel(input$entrega_insumos_xlsx$datapath, sheet = "agrochemicals"))), values_to = "quantidade", names_to = "cultura")
      dados_brutos <- dplyr::bind_rows(sementes, agroquimicos)
      dados_brutos <- dados_brutos %>% 
        mutate(sexo = ifelse(sexo == "0", NA, sexo),
               ano = ifelse(ano == 0, NA, ano),
               Entrega = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, Ano = ano, Localidade = localidade, Itens = items, `Insumo` = cultura,
                      Variedade = variedade, Unidade = unidade,  `Quantidade entregue` = quantidade, Entrega)
      
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    output$entrega_semente_novo <- renderDT({
      req(input$entrega_semente_xlsx)
      dados_brutos <- read_excel(input$entrega_semente_xlsx$datapath, sheet = "seed_deliveries", col_types =   coltypes_entrega_sementes) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        mutate(sexo = ifelse(sexo == "0", NA, sexo),
               ano = ifelse(ano == 0, NA, ano),
               Entrega = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de nascimento` = ano, Localidade = localidade, Povoado = povoado, Itens = items, `Cultura` = cultura,
                      Variedade = variedade, Unidade = unidade,  `Quantidade entregue` = quantidade, Entrega)
      
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    output$entrega_agroquimicos_novo <- renderDT({
      req(input$entrega_agroquimicos_xlsx)
      dados_brutos <- read_excel(input$entrega_agroquimicos_xlsx$datapath, sheet = "agrochemicals", col_types =   coltypes_entrega_agroquimicos) %>%
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>%
        pivot_longer(cols = c(11:ncol(read_excel(input$entrega_agroquimicos_xlsx$datapath, sheet = "agrochemicals"))), values_to = "quantidade", names_to = "Insumo") %>% 
        mutate(sexo = ifelse(sexo == "0", NA, sexo),
               ano = ifelse(ano == 0, NA, ano),
               Entrega = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de nascimento` = ano, Localidade = localidade, Povoado = povoado, Itens = items, Insumo,
                      `Quantidade entregue` = quantidade, Entrega)
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    output$vacinacao_newcastle_novo <- renderDT({
      req(input$vacinacao_newcastle_xlsx)
      
      dados_brutos <- read_excel(input$vacinacao_newcastle_xlsx$datapath, sheet = "newcastle", col_types =     coltypes_vacinacao_newcastle) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        mutate(sexo = ifelse(sexo == "0", NA, sexo),
               ano = ifelse(ano == 0, NA, ano),
               Entrega = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de nascimento` = ano, Localidade = localidade, Povoado = povoado, 
                      `Bicos arrolados` = arroladas_newcastle,	`Bicos vacinados` = vacinadas_newcastle,	`Preço por Bico (MT)` = newcastle_preco,	
                      `Valor pago (MT)` = newcastle_pago)
      
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    output$vacinacao_obrigatoria_novo <- renderDT({
      req(input$vacinacao_obrigatoria_xlsx)
      dados_brutos <- read_excel(input$vacinacao_obrigatoria_xlsx$datapath, sheet = "vacinacao_obrigatoria", col_types =coltypes_vacinacao_obrigatoria) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        pivot_longer(cols = c(carbunculo_hematico: felinos), names_to = "vacina", values_to = "Vacinados") %>% 
        dplyr::filter(!is.na(Vacinados)& Vacinados >0 & classe %in% c("vacinados", "VACINADOS")) %>% 
        mutate(
          especie = ifelse(vacina == "felinos", "Gatos", ifelse(vacina == "caninos", "Caninos", "Bovinos")),
          vacina = dplyr::case_when(vacina == 'carbunculo_hematico' ~ 'Carbunculo hemático', vacina == 'carbunculo_sintomatico' ~ 'Carbunculo sintomático', vacina == 'febre_aftosa' ~ 'Febre aftosa', vacina == 'dermatose_nodular' ~ 'Dermatose nodular', vacina =='brucelose' ~ 'Brucelose', vacina =='caninos' ~ 'Raiva', vacina =='felinos' ~ 'Raiva', TRUE  ~ vacina),
          sexo = ifelse(sexo == "0", NA, sexo),
          ano = ifelse(ano == 0, NA, ano),
          Entrega = format(delivery_date, "%d/%m/%Y")) %>% 
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de nascimento` = ano, Localidade = localidade, Povoado = povoado, 
                      `Espécies abrangidas` = especie,	Vacina = vacina,	`Animais vacinados` = Vacinados)
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
    })
    
    output$entrega_equipamento_novo<- renderDT({
      req(input$entrega_equipamento_xlsx)
        equipment_deliveries <- read_excel(input$entrega_equipamento_xlsx$datapath, sheet = "equipment_deliveries", col_types = coltypes_entrega_equipamento_unnormal) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        pivot_longer(13:ncol(read_excel(file_path2, sheet = "equipment_deliveries")), names_to = "variavel", values_to = "quantidade") %>% 
        dplyr::filter(quantidade != 0 & !is.na(quantidade))%>% left_join(dicionario, by = "variavel") %>% select(-variavel) %>% 
        rename(equipamento = label)
      normal_group_equipment <- read_excel(input$entrega_equipamento_xlsx$datapath, sheet = "normal_group_equipment", col_types = coltypes_entrega_equipamento_normal) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id))
      dados_brutos <- dplyr::bind_rows(equipment_deliveries, normal_group_equipment %>% mutate(ano = as.numeric(ano)))
      dados_brutos <- dados_brutos %>% dplyr::filter(!is.na(quantidade)) %>% 
        mutate(sexo = ifelse(sexo == "0", NA, sexo), 
               ano = ifelse(ano == 0, NA, ano), 
               Entrega = format(delivery_date, "%d/%m/%Y")) %>%
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de nascimento` = ano, 
                      Localidade = localidade, Povoado = povoado, `Organização de produtores` = group_id, 
                      `Equipamento alocado` = equipamento,	`Itens alocados` = quantidade)
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
      
    })
    
    output$treinamento_beneficiarios_novo <- renderDT({
      req(input$treinamento_beneficiarios_xlsx)
      dados_brutos <- read_excel(input$treinamento_beneficiarios_xlsx$datapath, sheet = "beneficiarios", col_types = coltypes_treinamento_beneficiarios) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(admin_id)) %>% 
        mutate(date_started = format(date_started, "%d/%m/%Y"),
               ano = ifelse(ano == 0, NA, ano),
               sexo = ifelse(sexo == "0", NA, sexo),
               role = ifelse(role == "0", NA, role),
               phone = ifelse(phone == "0", NA, phone),
               entidade = ifelse(entidade == "0", NA, entidade)) %>%
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `Ano de Nascimento`  = ano, Distrito = distrito, Entidade = entidade,
                      `Papel na Organização ou Entidade` = role, Telefone = phone, `Data do Evento` = date_started)
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
    })
    
    output$treinamento_tecnicos_novo <- renderDT({
      req(input$treinamento_tecnicos_xlsx)
      dados_brutos <- read_excel(input$treinamento_tecnicos_xlsx$datapath, sheet = "tecnicos", col_types = coltypes_treinamento_tecnicos) %>% 
        dplyr::filter(nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido) & !is.na(people_admin_id)) %>% 
        mutate(date_started = format(date_started, "%d/%m/%Y"),
               staff_id = ifelse(nuit == 0, NA, nuit),
               sexo = ifelse(sexo == "0", NA, sexo),
               role = ifelse(role == "0", NA, role),
               phone = ifelse(phone == "0", NA, phone),
               email = ifelse(email == "0", NA,email),
               entidade = ifelse(entidade == "0", NA, entidade)) %>%
        dplyr::select(Nome = nome, Apelido = apelido, Sexo = sexo, `NUIT`  = nuit, Distrito = distrito, Entidade = entidade,
                      `Papel na instituição` = role, Telefone = phone, Email = email, `Data` = date_started)
      
      datatable(dados_brutos, rownames= FALSE, options = list(pageLength = 5, initComplete = initComplete))
    })
    
    output$beneficiarios_registados <- renderValueBox({
      numero <- dbGetQuery(far_pool, SQL(paste0('SELECT COUNT (DISTINCT people_id) FROM vistas.outreach')))
      valueBox(tags$b(numero, style = "font-size: 300%;"), paste("Beneficiários alcançados"), icon=icon("user"), color = "success")
    })
    
    output$tecnicos_envolvidos <- renderValueBox({
      numero <- dbGetQuery(far_pool, SQL(paste0('SELECT COUNT  (DISTINCT staff_id) FROM vistas.staff_treinado')))
      valueBox(tags$b(numero, style = "font-size: 300%;"), paste("Técnicos envolvidos"), icon=icon("motorcycle"), color = "primary")
    })
    
    output$agentes_comunitarios <- renderValueBox({
      numero1 <- dbGetQuery(far_pool, SQL(paste0('SELECT COUNT  (DISTINCT people_id) FROM deliveries.agentes')))
      numero2 <- dbGetQuery(far_pool, SQL(paste0('SELECT COUNT  (DISTINCT people_id) FROM trained.agentes_de_apoio')))
      valueBox(tags$b(numero1+numero2, style = "font-size: 300%;"), paste("Agentes Comunitários"), icon=icon("bicycle"), color = "lime")
    })
    
    output$organizacoes_produtores <- renderValueBox({
      numero1 <- dbGetQuery(far_pool, SQL(paste0('SELECT COUNT  (DISTINCT id) FROM msme.msme')))
      valueBox(tags$b(numero1, style = "font-size: 300%;"), paste("Organizações de Produtores"), icon=icon("users-viewfinder"), color = "orange")
    })
    
    
    output$top_beneficiaries_plot <- renderPlot({
      outreach <- dbGetQuery(far_pool, SQL(paste0("SELECT 
      distrito,
      COUNT(DISTINCT people_id) AS beneficiarios
      FROM vistas.outreach WHERE intervencao NOTNULL AND intervencao != 'NA' AND intervencao LIKE '%PAC%'
      GROUP BY distrito, intervencao ORDER BY COUNT(DISTINCT people_id) DESC LIMIT 10;")))
      grafico_barras(outreach, distrito, beneficiarios, 35)
      
      })
   
    
    ###############  EXECUTAR ACÇÕES
    observeEvent(input$salvar_vacinacao_newcastle, {
      req(input$vacinacao_newcastle_xlsx)
      deliveries_uploads(input$vacinacao_newcastle_xlsx$datapath)
      
      coltypes_vacinacao_newcastle = c('text', 'text', 'date', 'numeric', 'numeric', 'text', 'numeric', 'text', 'text', 'text', 'numeric', 'text', 'text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric')
      
      vacinacao_newcastle <- read_excel(input$vacinacao_newcastle_xlsx$datapath, sheet = "newcastle", col_types =coltypes_vacinacao_newcastle) %>% 
        dplyr::filter(!is.na(admin_id) & nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido)) %>%
        mutate(nome_limpo = limpar_nomes(nome), apelido_limpo = limpar_nomes(apelido)) %>% 
        left_join(beneficiaries_id, by = c("people_admin_id", "nome_limpo", "apelido_limpo")) %>% 
        left_join(delivery_events_id, by = c("admin_id", "delivery_date", "items")) %>% 
        mutate(especie = "Galinhas") %>% 
        mutate(texto_arrolados_newcastle = paste0("(", people_id, ", '", especie,"', '", delivery_date,"', ", arroladas_newcastle, ")"),
               texto_vacinados_newcastle = paste0("(", people_id, ", ", event_id,", ", vacinadas_newcastle, ")"),
               texto_preco_newcastle = paste0("(", people_id, ", ", event_id,", ", newcastle_preco, ")"),
               texto_pagamentos_newcastle = paste0("(", people_id, ", ", event_id,", ", newcastle_pago, ")"))
      
      df_arrolados <- vacinacao_newcastle %>% dplyr::filter(arroladas_newcastle > 0 & !is.na(arroladas_newcastle))
      df_vacinados <- vacinacao_newcastle %>% dplyr::filter(vacinadas_newcastle > 0 & !is.na(vacinadas_newcastle))
      df_pagamentos <- vacinacao_newcastle %>% dplyr::filter(newcastle_pago > 0 & !is.na(newcastle_pago))
      df_precos <- vacinacao_newcastle %>% dplyr::filter(newcastle_preco > 0 & !is.na(newcastle_preco))
      
      texto_arrolados_newcastle <- glue_sql_collapse(df_arrolados$texto_arrolados_newcastle, sep = ", ", width = Inf, last = "")
      texto_vacinados_newcastle  <- glue_sql_collapse(df_vacinados$texto_vacinados_newcastle, sep = ", ", width = Inf, last = "")
      texto_pagamentos_newcastle  <- glue_sql_collapse(df_pagamentos$texto_pagamentos_newcastle, sep = ", ", width = Inf, last = "")
      texto_preco_newcastle  <- glue_sql_collapse(df_precos$texto_preco_newcastle, sep = ", ", width = Inf, last = "")
      
      tryCatch({
        dbGetQuery(far_pool, SQL(paste0('INSERT INTO beneficiaries.efectivos (people_id, especie, actualizacao, animais) VALUES', texto_arrolados_newcastle, " ON CONFLICT DO NOTHING;")))
      }, error = function(e) {return(0)})
      tryCatch({
        dbGetQuery(far_pool, SQL(paste0('INSERT INTO sanidade.newcastle (people_id, event_id, bicos) VALUES', texto_vacinados_newcastle, " ON CONFLICT DO NOTHING;")))
      }, error = function(e) {return(0)})
      tryCatch({
        dbGetQuery(far_pool, SQL(paste0('INSERT INTO sanidade.precos_newcastle (people_id, event_id, preco) VALUES', texto_preco_newcastle, " ON CONFLICT DO NOTHING;")))
      }, error = function(e) {return(0)})
      tryCatch({
        dbGetQuery(far_pool, SQL(paste0('INSERT INTO sanidade.pagamentos_newcastle (people_id, event_id, pago) VALUES', texto_pagamentos_newcastle, " ON CONFLICT DO NOTHING;")))
      }, error = function(e) {return(0)})
      
      
      progressSweetAlert(session = session, id = "myprogress", title = paste0("Inserindo ", nrow(dataset), " registos. Aguarde, por favor..."), display_pct = TRUE, value = 0)
      for (i in 1:nrow(dataset)){
        Sys.sleep(0.1)
        updateProgressBar(session = session, id = "myprogress", value = (i/nrow(dataset))*100)}
      closeSweetAlert(session = session)
      sendSweetAlert(session = session, title =paste0("<h3>SUCESSO!</h3><br>Inseridos ou actualizados  ", nrow(dataset)," formandos"), type = "success")
    })
   
    
    observeEvent(input$salvar_treinamento_beneficiarios, {
      bentrains_uploads(input$treinamento_beneficiarios_xlsx$datapath)
      
      put_object(file = input$treinamento_beneficiarios_xlsx$datapath, object = paste0(sessions$tema_central[1], "_", sessions$admin_id[1],  "_", sessions$date_started[1], "_BEN.xlsx"), bucket = "listasdiversas/treinamentos/digitadas")

      observe({if(isTruthy(input$treinamento_beneficiarios_pdf$datapath)) {
        put_object(file = input$treinamento_beneficiarios_pdf$datapath, object = paste0(sessions$tema_central[1], "_", sessions$admin_id[1],  "_", sessions$date_started[1], "_BEN.pdf"), bucket = "listasdiversas/treinamentos/pdfs")
      } else {NULL}})
      
      
      progressSweetAlert(session = session, id = "bentrains_progress", title = paste0("Inserindo ", nrow(dataset), " registos. Aguarde, por favor..."), display_pct = TRUE, value = 0)
      for (i in 1:nrow(dataset)){
        Sys.sleep(0.1)
        updateProgressBar(session = session, id = "bentrains_progress", value = (i/nrow(dataset))*100)}
      closeSweetAlert(session = session)
      sendSweetAlert(session = session, title =paste0("<h3>SUCESSO!</h3><br>Inseridos ou actualizados  ", nrow(dataset)," registos"), type = "success")
      
    })

    

    observeEvent(input$salvar_treinamento_tecnicos, {
      techtrains_uploads(input$treinamento_tecnicos_xlsx$datapath)
      
      put_object(file = input$treinamento_tecnicos_xlsx$datapath, object = paste0(coded_sessions$tema_central[1], "_", coded_sessions$admin_id[1],  "_", coded_sessions$date_started[1], "TEC_.xlsx"), bucket = "listasdiversas/treinamentos/digitadas")
      # put_object(file = input$treinamento_tecnicos_pdf$datapath, object = paste0(sessions$tema_central[1], "_", sessions$admin_id[1],  "_", sessions$date_started[1], "TEC_.pdf"), bucket = "listasdiversas/treinamentos/pdfs/")

      progressSweetAlert(session = session, id = "techtrains_progress", title = paste0("Inserindo ", nrow(staff), " registos. Aguarde, por favor..."), display_pct = TRUE, value = 0)
      for (i in 1:nrow(staff)){
        Sys.sleep(0.1)
        updateProgressBar(session = session, id = "techtrains_progress", value = (i/nrow(staff))*100)}
      closeSweetAlert(session = session)
      sendSweetAlert(session = session, title =paste0("<h3>SUCESSO!</h3><br>Inseridos ou actualizados  ", nrow(staff)," formandos"), type = "success")
      
    })
    
    
    observeEvent(input$salvar_entrega_animais, {
    designacao_ficheiro <- input$entrega_animais_xlsx$datapath
    deliveries_uploads(designacao_ficheiro)

    dataset <- read_excel(designacao_ficheiro, sheet = "livestock_deliveries", col_types = coltypes_entrega_animais) %>% 
        dplyr::filter(!is.na(admin_id) & nome != '0' & apelido != '0' & !is.na(nome) & !is.na(apelido)) %>%
        mutate(nome_limpo = limpar_nomes(nome), apelido_limpo = limpar_nomes(apelido), items = species) %>% 
        left_join(beneficiaries_id, by = c("people_admin_id", "nome_limpo", "apelido_limpo")) %>% 
        left_join(delivery_events_id, by = c("admin_id", "delivery_date", "items")) %>% 
        mutate(texto_animais = paste0("(", event_id, ", ", people_id, ", '", species,"', '", breed,"', '", category,"', '", generation,"', ",quantity, ")")) %>% 
        dplyr::filter(!is.na(people_id))
      
      texto_animais <- glue_sql_collapse(dataset$texto_animais, sep = ", ", width = Inf, last = "")
      dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.animais (event_id, people_id, especie, raca, categoria, geracao, animais) VALUES', texto_animais, " ON CONFLICT DO NOTHING;")))
      
      livestock_delivery_id <- dbGetQuery(far_pool, SQL(paste0("SELECT id as delivery_id, people_id, event_id, especie AS species, raca As breed, categoria AS category, geracao AS generation FROM deliveries.animais WHERE 
                                          event_id IN (", glue_sql_collapse(dataset$event_id, sep = ", ", width = Inf, last = ""), ")
                                          AND people_id IN (",  glue_sql_collapse(dataset$people_id, sep = ", ", width = Inf, last = ""), ")  
                                          AND especie IN ('",  glue_sql_collapse(dataset$species, sep = "', '", width = Inf, last = ""), "')  
                                          AND raca IN ('",  glue_sql_collapse(dataset$breed, sep = "', '", width = Inf, last = ""), "')  
                                          AND geracao IN ('",  glue_sql_collapse(dataset$generation, sep = "', '", width = Inf, last = ""), "')  
                                          AND categoria IN ('", glue_sql_collapse(dataset$category, sep = "', '", width = Inf, last = ""), "');")))
      
      dataset$brincos <- gsub(',',';',dataset$brincos, fixed = TRUE)
      
      livestock_brincos <- dataset %>% select(people_id, event_id, brincos, breed, species, generation, category, curral)
      livestock_brincos <- cSplit(livestock_brincos, "brincos", ";")
      livestock_brincos <- livestock_brincos %>% 
        pivot_longer(cols = c(starts_with('brincos')), names_to = "contagem", values_to = "brinco") %>% 
        dplyr::filter(!is.na(brinco) & brinco != "0") %>% 
        left_join(livestock_delivery_id, by = c("people_id", "event_id", "species", "breed", "category", "generation")) %>% 
        mutate(texto_brincos = paste0("(", delivery_id, ", '", brinco,"')"),
               texto_currais = paste0("(", delivery_id, ", '", curral,"')"))
      texto_texto_brincos <- glue_sql_collapse(livestock_brincos$texto_brincos, sep = ", ", width = Inf, last = "")
      texto_currais <- glue_sql_collapse(livestock_brincos$texto_currais, sep = ", ", width = Inf, last = "")
      dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.brincos_animais (animais_id, brinco) VALUES', texto_texto_brincos, " ON CONFLICT DO NOTHING;")))
      dbGetQuery(far_pool, SQL(paste0('INSERT INTO deliveries.currais (animais_id, curral) VALUES', texto_currais, " ON CONFLICT DO NOTHING;")))

      put_object(file = designacao_ficheiro, object = paste0(delivery_events_id$items[1], "_", delivery_events_id$admin_id[1],  "_", delivery_events_id$date_started[1], "Animais_.xlsx"), bucket = "listasdiversas/entregas/digitadas/")
      progressSweetAlert(session = session, id = "deliveries_progress", title = paste0("Inserindo ", nrow(dataset), " registos. Aguarde, por favor..."), display_pct = TRUE, value = 0)
      for (i in 1:nrow(dataset)){
        Sys.sleep(0.1)
        updateProgressBar(session = session, id = "deliveries_progress", value = (i/nrow(dataset))*100)}
      closeSweetAlert(session = session)
      sendSweetAlert(session = session, title =paste0("<h3>SUCESSO!</h3><br>Inseridos ou actualizados  ", nrow(dataset)," registos"), type = "success")
      
    })
    
    
    outreach <- reactive({
      outreach <- dbGetQuery(far_pool, SQL(paste0("SELECT 
      CASE WHEN COALESCE(intervencao_detalhada, intervencao) ISNULL THEN 'Outras' ELSE COALESCE(intervencao_detalhada, intervencao) END AS intervencao,
      provincia, distrito, localidade, latitude,longitude,
      COUNT(DISTINCT people_id) as beneficiarios  
      FROM vistas.outreach WHERE provincia NOTNULL
      GROUP BY  COALESCE(intervencao_detalhada, intervencao), provincia, 
      distrito, localidade, latitude, longitude;")))
      return(outreach)
    })
    
    # output$cobertura_geografica <- renderTmap({
    # 
    #   dataset <- as.data.frame(outreach())
    #   
    #   data_sf <- st_as_sf(x=dataset[ which(!is.na(dataset$latitude) & !is.na(dataset$longitude)),], coords = c("longitude", "latitude"), crs= "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
    #   
    #   beneficiaries_map <- mozpostos_wgs84 %>% st_join(., data_sf)
    #   st_geometry(beneficiaries_map) <- NULL
    #   Cobertura <- beneficiaries_map %>%
    #     group_by(id, localidade, distrito, provincia) %>%
    #     summarize(Beneficiarios = sum(beneficiarios)) %>%
    #     ungroup() %>% left_join(mozpostos_wgs84, .)
    #   Limites <- Distritos
    #   Cobertura$Beneficiarios <- round(ifelse(is.na(Cobertura$localidade),NA,Cobertura$Beneficiarios), digits = 0)
    # 
    #   tm_shape(Cobertura, is.master = FALSE, units = 'm') + tm_borders(alpha = 0)+
    #     tm_polygons("Beneficiarios",
    #                 id = toupper("ADM3_PT"),
    #                 palette = "YlOrRd", n = 9, contrast = c(0.3, 1),
    #                 breaks = Breaks,
    #                 borders = NULL,
    #                 colorNA = NULL,
    #                 legend.is.portrait = FALSE,
    #                 labels = Labels,
    #                 popup.vars=c("Distrito:"="ADM2_PT", "Posto Administrativo:"="ADM3_PT", "Beneficiários:"="Beneficiarios"),
    #                 style = "fixed",
    #                 title = "Beneficiários",
    #                 alpha = 0.5,
    #                 
    #                 legend.stack = "horizontal")+
    #     
    #     tm_layout(legend.outside.position = "bottom",
    #               legend.outside.size = 0.35,
    #               legend.outside = TRUE)+
    #     tm_fill(legend.is.portrait = FALSE,
    #             colorNA = "#ffffff", textNA = "Sem assistência")+
    #     
    #     qtm(Distritos,
    #         fill = NULL,
    #         text = "ADM2_PT",
    #         text.size = 1.7,
    #         text.col = "#A9A9A9",
    #         fillCol = "ADM1_PT",
    #         borders = "#C0C0C0", scale = 0.4, polygons.id = "ADM2_PT")+
    #     
    #     qtm(Limites,
    #         fill = NULL,
    #         fillCol = "ADM1_PT",
    #         borders = "#C0C0C0", scale = 0.4)+ tm_view(set.view = c(36, -13.8287, 7.5))+
    #     
    #     tm_layout(legend.outside.position = "bottom",
    #               legend.outside.size = 0.35,
    #               legend.outside = TRUE)
    # })
    # 

    # observe({
    #   session$sendCustomMessage(
    #     "box_title",
    #     paste0("Currently showing variable:", input$var)
    #   )
    # })


    
  }
)