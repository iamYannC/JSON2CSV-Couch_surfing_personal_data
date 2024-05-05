library(shiny)
library(jsonlite)
library(tidyverse)
library(glue)
library(bslib)

ui <- page_sidebar(
  sidebar = sidebar(
    fg = "#F07B48",bg = "#F6E3DA",
  titlePanel("JSON to CSV Converter"),
  fileInput("jsonFile", "Upload JSON File"), # change bg color
  downloadButton("downloadCSV", "Download CSVs"),open = T

  ),
  fg = "#F07B48",bg = "white",
      h3(HTML("Hi, Sorry for lack of UI. Upload the .json file with your data from CS and it'll generate it for you in a nice xl format.<br><br>
         If you dont have that file, go to couchsurfing (can click on the logo) and press 'Manage Account', then request your data.
         It may take a few days for them to generate it for you. <br>
         <b>Any file that you upload here is not stored or used in any way.</b> <br>")),
  # add an image, src url:
  # add href to the image

  div(
    a(href = "https://www.couchsurfing.com/products/membership", target = "_blank",
    img(src = "https://upload.wikimedia.org/wikipedia/commons/8/86/Couchsurfing_logo.png",height = 150, width = 700),
      # set transparency
      style = "opacity: 0.15; filter: alpha(opacity=50);")
  # add a link to the image
)
)


server <- function(input, output) {


  # Read and process the uploaded JSON file
  json_data <- reactive({
    req(input$jsonFile)
    inFile <- input$jsonFile
    jsonlite::fromJSON(inFile$datapath)
  })
  # Generate CSV files from the list of data frames
  generate_csvs <- function(json) {

    ##### User Data ####
    my_id <- json$user_data$id
    ##### Friends ####
    all_friends <- json$friends |>  str_extract_all("\\d+") |> unlist()

    ##### Messages ####

    tmp.msg_level <- json$messages[[1]] |>select(1,4) |> unnest(messages)
    tmp.msg_level <- tmp.msg_level |> select(user_ids_concatenated,message_thread_id,author_id,body,created_at)

    tmp.conversdation_level <- json$messages[[1]] |>select(4) |> unnest(messages) |> select(2)
    tmp.conversation_level <- tmp.conversdation_level$system_message$a|> as_tibble()

    all_msgs <- bind_cols(tmp.msg_level,tmp.conversation_level)



    ##### References ####
    tmp.ref_received <-json$references$received_references |> mutate(ref_type="received")
    tmp.ref_written <-json$references$written_references |> mutate(ref_type="written")

    all_ref <- bind_rows(tmp.ref_received,tmp.ref_written)


    ##### Events ####
    tmp.event_types <- json$events |> names() |> str_remove("events_")
    get_events <- function(i){
      json$events[[i]]$location |>
        mutate(
          event_type = tmp.event_types[[i]],
          name = json$events[[i]]$name,
          description = json$events[[i]]$description,
          .before = everything()
        )
    }
    tmp.all_events <- map_dfr(1:length(tmp.event_types),get_events)


    ##### Locations ####
    all_events <- bind_rows(json$locations$profile_locations,tmp.all_events)

    ##### Posts ####

    tmp.idx_not_null <- which(map_dbl(json$posts,length)!=0)

    get_posts <- function(i){

      idx_real <- as.numeric(tmp.idx_not_null[i])
      post_type = names(tmp.idx_not_null[i])

      json$posts[[idx_real]] |>
        mutate(post_type = post_type)
    }
    all_posts <- map_dfr(1:length(tmp.idx_not_null),get_posts) |> relocate(post_type,.before = everything()) |> tibble()

    ##### Public Trips ####

    tmp.without_loc <- 4
    tmp.trips <-
      map_dfc(1:tmp.without_loc,\(s) json$public_trips$public_trips[[s]])|> set_names(names(json$public_trips$public_trips)[1:tmp.without_loc])

    # now add location data

    # start with the annoying ones:
    tmp.add_obj <- json$public_trips$public_trips[[5]]$address_object

    get_value_trip <- function(obj_name){
      temp <- rep(NA,54)
      if(obj_name=='geometry'){
        indx <- which(map_lgl(tmp.add_obj[[obj_name]][,2],is_empty))
        temp[-indx] <- map_chr(tmp.add_obj[[obj_name]][,2][-indx],\(s) str_c(s,collapse = ","))
        return(temp)
      }
      indx <- which(map_lgl(tmp.add_obj[[obj_name]],is_empty))

      temp[-indx] <- unlist(map(tmp.add_obj[[obj_name]][-indx],\(s) str_c(s,collapse = ",")))
      return(temp)
    }

    # after defining a function to gather annoying cols, make a data frame of the atomic vectors:
    tmp.vec_indexes <- which(!map_chr(tmp.add_obj,class) %in% c("list","data.frame"))
    tmp.location_address <- tmp.add_obj[tmp.vec_indexes] |> tibble()

    # bind cols
    tmp.location_address <-
      tmp.location_address |> bind_cols(
        tibble(
          place_type = get_value_trip('place_type'),
          bbox = get_value_trip('bbox'),
          center = get_value_trip('center'),
          geometry = get_value_trip('geometry'),
          context = get_value_trip('context')
        )
      )

    tmp.trips_w_meta_data <- tmp.trips |> bind_cols(tmp.location_address)

    all_trips <- tmp.trips_w_meta_data |> select(1,2,4,formatted_address)
    all_locations <- tmp.trips_w_meta_data |> select(5:length(tmp.trips_w_meta_data))


    ##### Couch Visits ####
    all_couch_visits <-
      bind_rows(
        bind_cols(
          json$couch_visits$surfer_couch_visits[c(2,3)],
          display_name = json$couch_visits$surfer_couch_visits$host$profile$display_name,
          country = json$couch_visits$surfer_couch_visits$location$country,
          city = json$couch_visits$surfer_couch_visits$location$city,
          type = "surfer",
          id = as.character(json$couch_visits$surfer_couch_visits$host$profile$id)
        ),
        # Bind surfer & host data
        bind_cols(
          json$couch_visits$host_couch_visits[c(2,3)],
          display_name = json$couch_visits$host_couch_visits$surfer$profile$display_name,
          country = json$couch_visits$host_couch_visits$location$country,
          city = json$couch_visits$host_couch_visits$location$city,
          type = "host",
          id = as.character(json$couch_visits$host_couch_visits$surfer$profile$id)
        )
      )


    # DATA WRANGLING & FEATURE ENGINEERING ------------------------------------
    all_list_ <- list(
      all_couch_visits,
      all_events,
      all_friends,
      all_locations,
      all_msgs,
      all_posts,
      all_ref,
      all_trips
    ) |> set_names(c(
      "all_couch_visits",
      "all_events",
      "all_friends",
      "all_locations",
      "all_msgs",
      "all_posts",
      "all_ref",
      "all_trips"
    )) # make sure i never name all_list_ because it doesnt end with a letter

    #### Date formatting ####

    # The following locations in my list have the column arrival. I will use this code to detect all other date columns and format them as necessary
    with_arrival_col <- which(map_lgl(map(all_list_,colnames), \(s) s |> str_detect("arrival") |> any()) )

    all_list_[with_arrival_col] <-
      map(all_list_[with_arrival_col],\(df) df |>
            mutate(across(c(arrival,departure), as.Date)))


    with_created_col <- which(map_lgl(map(all_list_,colnames), \(s) s |> str_detect("created") |> any()) )

    all_list_[with_created_col] <-
      map(all_list_[with_created_col] ,\(df) {
        df |> mutate(date = as.Date(created_at),
                     time = created_at |> ymd_hms() |> format( format = "%H:%M:%S")
        ) |> select(-created_at)
      }
      )

    with_resp_created_col <- which(map_lgl(map(all_list_,colnames), \(s) s |> str_detect("response_created") |> any()) )

    all_list_[with_resp_created_col] <-
      map(all_list_[with_resp_created_col] ,\(df) {
        df |> mutate(response_date = as.Date(response_created_at),
                     response_time = response_created_at |> ymd_hms() |> format( format = "%H:%M:%S")
        ) |> select(-response_created_at)
      }
      )

    all_list_ <- map(all_list_,tibble)


    #### Users [modified couch visits and friends] ####
    all_friends <- all_list_$all_friends |> set_names("id")
    users <-
      bind_rows(
        bind_cols(id = all_friends$id, type = "friend"),

        bind_cols(
          display_name = json$couch_visits$surfer_couch_visits$host$profile$display_name,
          username = json$couch_visits$surfer_couch_visits$host$username,
          id = as.character(json$couch_visits$surfer_couch_visits$host$profile$id),
          type = "host"
        ),

        bind_cols(
          display_name = json$couch_visits$host_couch_visits$surfer$profile$display_name,
          username = json$couch_visits$host_couch_visits$surfer$username,
          id = as.character(json$couch_visits$host_couch_visits$surfer$profile$id),
          type = "surfer"
        )
      )

    users <-
      users |>
      mutate(type = paste(unique(type),collapse = " & "),
             display_name = paste(unique(display_name),collapse = "") |> str_remove("NA"),
             username = paste(unique(username),collapse = "") |> str_remove("NA"),
             .by = id) |> distinct() |>
      mutate(across(everything(),\(s) na_if(s,"") |> str_squish()))
    # to match with couch visits, update existing all_couch_visits instead of creating YAV (yet another variable)
    all_list_$all_couch_visits <- all_list_$all_couch_visits |> left_join(users |> select(id,username))
    #### Messages ####
    tmp.msgs <- all_list_$all_msgs |> drop_na(body) |>
      mutate(msg_partner = str_remove(user_ids_concatenated,as.character(my_id)) |>
               str_extract("\\d+"),
             msg_author = ifelse(author_id==my_id,"Me",msg_partner)
      ) |>
      select(msg_partner,msg_date = date,msg_time = time, msg_author,msg_body = body,
             req_arrival = arrival, req_departure = departure, req_accepted = status
      ) |> mutate(statuss = n_distinct(req_accepted,na.rm = T),.by = msg_partner) |>
      # fill NA of req_status with the complete cases value per each msg_partner
      mutate(
        req_accepted = case_when(
          is.na(req_accepted)~unique(req_accepted[complete.cases(req_accepted)][1]),
          statuss==2~1,
          TRUE~req_accepted
        ) |> as.logical(),
        .by = msg_partner) |> select(-statuss)

    tmp.join <- tmp.msgs |> left_join(all_list_$all_couch_visits |> select(display_name,id) |> distinct(), by = join_by(msg_partner ==id)) |>
      mutate(msg_author = ifelse(complete.cases(display_name) & msg_author!='Me',display_name,msg_author),
             msg_body = str_trim(msg_body)) # trim to not lose formatting within msgs

    all_list_$all_msgs <- tmp.join |> select(msg_author,msg_body,msg_date,msg_time,req_arrival,req_departure,req_accepted,msg_partner) |> rename(msg_partner_id = msg_partner)

    #### References ####
    tmp.ref <- all_list_$all_ref |> mutate(across(c(creator_id,recipient_id),as.character))


    tmp.ref <-
      tmp.ref |>
      drop_na(body) |>
      mutate(ref_partner_id =paste0(creator_id,recipient_id) |> str_remove(as.character(my_id)),
             ref_author = ifelse(ref_partner_id==creator_id,ref_partner_id,"Me"),
             .before = everything()) |>
      select(ref_author,ref_body = body,ref_date = date,ref_time = time,
             response_body, response_date, response_time,ref_experience = experience,
             ref_type = reference_type,ref_partner_id)

    tmp.join <-
      tmp.ref |>  left_join(all_list_$all_couch_visits |> select(display_name,id) |> distinct(), by = join_by(ref_partner_id ==id)) |>
      mutate(ref_author = ifelse(complete.cases(display_name) & ref_author!='Me',display_name,ref_author),
             ref_body = str_trim(ref_body))

    all_list_$all_ref <- tmp.join |> select(-display_name)

    #### Location metadata [replaced locations & events] ####

    all_locations <- all_list_$all_locations

    tmp.locations <-
      all_locations |> mutate(administrative_area_level_2 =
                                case_when(
                                  complete.cases(administrative_area_level_3)~paste(administrative_area_level_2,administrative_area_level_3,collapse = ","),
                                  complete.cases(sublocality_level_1 )~paste(administrative_area_level_2,sublocality_level_1 ,collapse = ","),
                                  .default = administrative_area_level_2
                                ),
                              .by = place_id)
    # Lots of col removal
    tmp.locations <-
      tmp.locations |> select(locality:administrative_area_level_2,id,text,place_name:context) |>
      mutate(locality = case_when(is.na(locality)~text, .default = locality),
             formatted_address = case_when(is.na(formatted_address)~place_name, .default = formatted_address),
             administrative_area_level_2 = case_when(is.na(administrative_area_level_2)~text, .default = administrative_area_level_2),
             country = case_when(is.na(country)~gsub(".*,(.*)$", "\\1", place_name) |> trimws(), .default = country),
             administrative_area_level_1  = case_when(is.na(administrative_area_level_1)~gsub("^.*,(.*),.*$", "\\1", place_name) |> trimws() |>
                                                        # remove all text after the first comma
                                                        str_remove(",.*"), .default = administrative_area_level_1)
      ) |> select(-c(id:place_type))

    tmp.more_locations <- json$couch_visits$surfer_couch_visits$location
    tmp.more_locations <-
      tmp.more_locations |> tibble() |>
      mutate(country=case_when(country=="+61"~"AUS",
                               city == "Sydney"~"AUS",
                               .default = country),
             state = case_when(state=="NSW"~"New South Wales",
                               state=="Central district"~"Center District",
                               state=="Yucatán"~"Yucatan",
                               state %in% c("Tx", "TX")~"Texas",
                               str_detect(state,"Sacatepéquez")~"Sacatepequez",
                               str_detect(state,"Petén")~"Peten",
                               str_detect(state,"Cayo")~"Cayo",
                               state=="Yukon Territory"~"Yukon",
                               state=="Petén"~"Peten",
                               state=="puebla"~"Puebla",
                               city == "Sydney"~"New South Wales",
                               city == "Be'er Sheva"~"South District",
                               .default = state) |>
               str_to_title(),
             city = ifelse(city=='Beersheba', "Be'er Sheva",city)
      )

    tmp.more_loc_2 <- json$couch_visits$host_couch_visits$location |> select(country,state,city,latitude,longitude,display_name)

    tmp.loc_events <- all_list_$all_events |> select(locationable_id,address1:user_entered_text,place_id:display_name ) |>
      mutate(
        across(c(locationable_id,place_id),as.character),
        across(everything(),\(s) na_if(s,"") |> str_squish())) |>
      mutate(state = case_when(
        state=='LA'~"Louisiana",
        state=='IL'~"Illinois",
        .default = state
      ))


    # some final adjustmenets
    tmp.loc <- bind_rows(tmp.more_locations,tmp.more_loc_2,tmp.loc_events) |> distinct() |>
      mutate(across(everything(),\(s) na_if(s,"") |> str_squish())) |>
      rename(postal_code = zip) |> select(-c(address1,address2)) |>
      mutate(state = case_when(
        city == 'Victoria' & country == 'CAN'~"British Columbia",
        city == 'San Francisco' ~ "California",
        city == 'Sydney' ~ "New South Wales",
        city == 'Southbank' ~ "Victoria",
        city == 'Montréal' ~ "Quebec",
        city == 'Summer Hill' ~ "New South Wales",
        .default = state)
      )

    tmp.locations <- tmp.locations |> mutate(country = case_when(
      country == "AU"~"AUS",
      country == "CA"~"CAN",
      country == "US"~"USA",
      country == "MX"~"MEX",
      .default = country)
    ) |> rename(city=locality,state = administrative_area_level_1,county = administrative_area_level_2)
    map(list(tmp.loc,tmp.locations),colnames)

    all_list_$location_meta <-
      bind_rows(tmp.locations,tmp.loc) |>
      select(country,state,county,city,formatted_address,display_name,postal_code, latitude,longitude,everything()) |> rowwise() |>
      mutate(formatted_address = case_when(
        complete.cases(display_name)~display_name,
        complete.cases(postal_code)~paste(formatted_address,unique(postal_code),collapse = ", "),
        .default = formatted_address)) |>
      mutate(longitude  = ifelse(is.na(longitude),
                                 str_extract(geometry,"^[^,]*"),
                                 longitude),
             latitude = ifelse(is.na(latitude),
                               str_extract(geometry,"[^,]*$"),
                               latitude)
      ) |> # do again attaching postal_code to formatted address
      mutate(place_id = ifelse(is.na(place_id) & complete.cases(locationable_id),locationable_id ,place_id),
             formatted_address = ifelse(formatted_address!=user_entered_text,
                                        paste(formatted_address,user_entered_text,collapse = ", "),
                                        formatted_address)
      ) |>

      ungroup() |>
      select(-c(postal_code,display_name,center,geometry,user_entered_text,locationable_id)) |> arrange(longitude) |> distinct()

    # this replaces all_events & all_locations

    #### Posts ####
    all_list_$all_posts <- all_list_$all_posts |> select(-c(author_id,comment_thread_id,updated_at,deleted_at))

    #### Public Trips ####

    # Join some from public trips
    tmp.trips <- all_list_$all_trips
    tmp.trips <- tmp.trips |>
      separate_wider_delim(cols=formatted_address,delim = ",",names  = c("city","state","country","other"),
                           too_few = "align_start") |>
      mutate(across(everything(),str_squish)) |>
      mutate(
        state = case_when(
          # complete.cases(other)~"BC",
          state == 'Australia'~"New South Wales",
          state == 'QC'~"Quebec", state == 'BC'~"British Columbia", state == 'ON'~"Ontario", state == 'AB'~"Alberta", state == 'WA'~"Washington",
          str_detect(state,'CA')~"California",
          state == 'TX'~"Texas",
          state == 'NV'~"Nevada",
          state == 'IL'~"Illinois",
          state == 'AZ'~"Arizona",
          str_detect(state,'NM')~"New Mexico",
          state == 'LA'~"Louisiana",
          str_detect(state,'CO')~"Colorado",
          .default = state

        ),
        country =
          case_when(
            state=='New South Wales'~"AUS",
            # complete.cases(other)~"CAN",
            country == 'US'~"USA",
            country == 'Canada'~"CAN",
            country == 'Mexico'~"MEX",
            state == 'Mexico'~"MEX",

            .default = country
          ),
        city = ifelse(city=='Sydney NSW 2000', "Sydney",city)
      ) |> select(-other) |> distinct()


    all_list_$all_trips <- tmp.trips

    #### Update location meta ####

    all_list_$location_meta <- all_list_$location_meta |> full_join(select(tmp.trips,city:country) |> distinct())

    # TABLES SORTING ----------------------------------------------------------
    all_list_sorted <- list(
      user = all_list_$all_couch_visits,
      msgs = all_list_$all_msgs,
      posts = all_list_$all_posts,
      refs = all_list_$all_ref,
      trips = all_list_$all_trips,
      location_meta = all_list_$location_meta |> mutate(across(longitude:latitude,as.numeric))
    )


    map(1:length(all_list_sorted), \(indx) write_csv(all_list_sorted[[indx]],paste0(names(all_list_sorted)[indx],".csv")))
  }

  # Create a zip file containing the generated CSVs
  output$downloadCSV <- downloadHandler(
    filename = "couchsurfing_data.zip",
    content = function(file) {
      temp_dir <- tempdir()
      setwd(temp_dir)
      generate_csvs(json_data())  # Generate CSVs from the list of data frames
      zip_file <- paste0(temp_dir, "/couchsurfing_data.zip")
      zip(zip_file, files = list.files(temp_dir, pattern = "*.csv"))
      file.copy(zip_file, file)
    }
  )
}

shinyApp(ui, server)

