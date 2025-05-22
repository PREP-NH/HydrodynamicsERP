# app.R - Great Bay Particle Animation Viewer

library(shiny)
library(leaflet)
library(dplyr)
library(jsonlite)
library(shinyjs)
library(shinyWidgets)
library(DT)
library(stringr)
library(ggplot2)
library(plotly)
library(httr)
library(tidyr)

# Setup resource paths
addResourcePath("videos", "output/videos")

# Helper functions
`%||%` <- function(a, b) if (is.null(a)) b else a
safe_get <- function(x, name) if (is.list(x) && name %in% names(x)) x[[name]] else NULL

# Load configurations
load_configurations <- function() {
  tryCatch({
    particle_path <- "output/configs/particle_animation_metadata.json"
    if (!file.exists(particle_path)) {
      warning("Metadata file not found at: ", particle_path)
      return(list(sites_df = data.frame(), video_info = list(), variables = list()))
    }
    pdata <- fromJSON(particle_path, simplifyVector = FALSE)
    files_list <- safe_get(pdata, "files") %||% list()
    variables_list <- safe_get(pdata$animation_info, "variables") %||% list()

    # Build sites_df
    sites_df <- tibble::tibble(
      site_name = character(),
      display_name = character(),
      site_type = character(),
      longitude = double(),
      latitude = double(),
      color = character(),
      feature_code = character(),
      feature_name = character(),
      min_date = character(),
      max_date = character()
    )
    processed_sites <- character()

    for (item in files_list) {
      site_name <- safe_get(item, "site")
      if (is.null(site_name) || site_name == "" || site_name %in% processed_sites) next
      lon_range <- safe_get(item, "lon_range")
      lat_range <- safe_get(item, "lat_range")
      color <- safe_get(item, "color") %||% NA_character_
      site_type <- ifelse(grepl("^GB", site_name), "GB", "tier2")
      lon <- if (is.list(lon_range) && length(lon_range)>0) unlist(lon_range)[1] else NA_real_
      lat <- if (is.list(lat_range) && length(lat_range)>0) unlist(lat_range)[1] else NA_real_

      # Get display name, feature code and feature name
      display_name <- safe_get(item, "display_name") %||% site_name
      feature_code <- safe_get(item, "feature_code") %||% NA_character_
      feature_name <- safe_get(item, "feature_name") %||% NA_character_
      min_date <- safe_get(item, "min_date") %||% NA_character_
      max_date <- safe_get(item, "max_date") %||% NA_character_

      sites_df <- dplyr::add_row(
        sites_df,
        site_name = site_name,
        display_name = display_name,
        site_type = site_type,
        longitude = lon,
        latitude = lat,
        color = color,
        feature_code = feature_code,
        feature_name = feature_name,
        min_date = min_date,
        max_date = max_date
      )
      processed_sites <- c(processed_sites, site_name)
    }

    # Build video_info
    video_info <- list()
    for (item in files_list) {
      site_name <- safe_get(item, "site")
      if (is.null(site_name) || site_name == "") next
      filename <- safe_get(item, "filename")
      date <- safe_get(item, "date") %||% "unknown"
      is_alltstride <- safe_get(item, "is_alltstride") %||% FALSE
      display_name <- safe_get(item, "display_name") %||% site_name
      variables <- safe_get(item, "variables") %||% list()
      min_date <- safe_get(item, "min_date") %||% NA_character_
      max_date <- safe_get(item, "max_date") %||% NA_character_
      video_info[[site_name]] <- c(video_info[[site_name]],
        list(list(
          filename = filename,
          date = date,
          is_alltstride = is_alltstride,
          display_name = display_name,
          variables = variables,
          min_date = min_date,
          max_date = max_date,
          metadata = item
        ))
      )
    }

    list(sites_df = sites_df, video_info = video_info, variables = variables_list)
  }, error = function(e) {
    message("Error loading configurations: ", e$message)
    list(sites_df = tibble::tibble(), video_info = list(), variables = list())
  })
}

# Function to fetch data from ODM2 database via PostgREST API
# Updated code for the fetch_variable_data function and variableDataTable render function
# to use the correct column name 'variablenamecv' instead of 'variablename'

# Updated fetch_variable_data function
fetch_variable_data <- function(site_code, variable_name, start_date, end_date) {
  # Base URL for PREP API
  base_url <- "http://data.prepestuaries.org:3001"

  # First, we need to get the resultid for the site and variable
  endpoint <- paste0("/sitesummaries?samplingfeaturecode=eq.", site_code)
  message("Fetching site summary from: ", base_url, endpoint)

  # Make the API request to get site summary
  tryCatch({
    response <- GET(paste0(base_url, endpoint))
    if (http_status(response)$category == "Success") {
      site_data <- fromJSON(content(response, "text", encoding="UTF-8"), simplifyVector=TRUE)

      if (length(site_data) == 0 || nrow(site_data) == 0) {
        message("No site data found for: ", site_code)
        return(NULL)
      }

      # Find the appropriate result IDs based on variable name
      result_ids <- NULL
      if ("resultid" %in% names(site_data)) {
        # Some sites might have multiple results
        result_ids <- site_data$resultid

        # If we have variable information, try to filter by that
        # Use variablenamecv instead of variablename
        if ("variablenamecv" %in% names(site_data) && "variablecode" %in% names(site_data)) {
          # Case-insensitive matching for variable name or code
          var_matches <- grepl(variable_name, site_data$variablenamecv, ignore.case = TRUE) |
                          grepl(variable_name, site_data$variablecode, ignore.case = TRUE)

          if (any(var_matches)) {
            result_ids <- site_data$resultid[var_matches]
          }
        }
      }

      if (is.null(result_ids) || length(result_ids) == 0) {
        message("No result IDs found for variable: ", variable_name)
        message("Attempting to query by specific result ID for the site")
        result_ids <- site_data$resultid
      }

      # Query the timeseriesresultvalues table for each result ID
      all_data <- list()

      for (result_id in result_ids) {
        endpoint <- paste0("/timeseriesresultvalues?resultid=eq.", result_id)

        # Add date filters if provided
        if (!is.null(start_date) && !is.null(end_date)) {
          endpoint <- paste0(endpoint, "&valuedatetime=gte.", start_date, "&valuedatetime=lte.", end_date)
        }

        message("Fetching time series data from: ", base_url, endpoint)

        response <- GET(paste0(base_url, endpoint))
        if (http_status(response)$category == "Success") {
          data <- fromJSON(content(response, "text", encoding="UTF-8"), simplifyVector=TRUE)

          if (length(data) > 0 && nrow(data) > 0) {
            # Convert datetime to proper format
            data$valuedatetime <- as.POSIXct(data$valuedatetime)

            # Add additional metadata to the data
            if (nrow(site_data) > 0) {
              # Find the matching row in site_data
              match_idx <- which(site_data$resultid == result_id)[1]
              if (!is.na(match_idx)) {
                # Add variable name and units if available - use variablenamecv
                if ("variablenamecv" %in% names(site_data)) {
                  data$variablename <- site_data$variablenamecv[match_idx]
                } else {
                  data$variablename <- variable_name  # Default to selected variable
                }
                if ("unitsname" %in% names(site_data)) {
                  data$unitsname <- site_data$unitsname[match_idx]
                } else {
                  data$unitsname <- "unknown"  # Default unit
                }
                if ("variablecode" %in% names(site_data)) {
                  data$variablecode <- site_data$variablecode[match_idx]
                } else {
                  data$variablecode <- as.character(result_id)  # Use result ID as code
                }
              }
            }

            all_data[[length(all_data) + 1]] <- data
          }
        }
      }

      # Combine all data frames
      if (length(all_data) > 0) {
        combined_data <- do.call(rbind, all_data)
        return(combined_data)
      } else {
        message("No time series data found for the selected variable and date range")
        return(NULL)
      }
    } else {
      message("API request failed: ", http_status(response)$message)
      return(NULL)
    }
  }, error = function(e) {
    message("Error fetching data: ", e$message)
    return(NULL)
  })
}

# UI Definition
ui <- fluidPage(
  useShinyjs(),
  tags$script(
    "$(document).on('keydown', function(e){Shiny.setInputValue('ctrl_pressed', e.ctrlKey);});
     $(document).on('keyup',   function(e){Shiny.setInputValue('ctrl_pressed', false);});"
  ),
  tags$head(
    tags$style(HTML("
      .leaflet-container { background: #f2f2f2; }
      .video-container { width: 100%; max-width: 800px; margin: 0 auto; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-radius: 4px; overflow: hidden; }
      #videoPlayer { width: 100%; max-height: 450px; }
      .status-message { padding: 15px; margin-bottom: 20px; border-radius: 4px; background-color: #f8f9fa; border-left: 5px solid #6c757d; }
      .site-count { font-size: 0.9rem; color: #666; margin-top: 5px; }
      .site-info { background-color: #f8f9fa; padding: 15px; margin-bottom: 15px; border-radius: 4px; }
      .variables-table { margin-top: 15px; }
      .info-panel { background-color: #e7f3ff; border: 1px solid #b3d9ff; border-radius: 4px; padding: 15px; margin-bottom: 20px; }
      .info-panel h4 { color: #0056b3; margin-top: 0; }
      .animation-specs { background-color: #f8f9fa; border-left: 4px solid #007bff; padding: 10px 15px; margin: 10px 0; }
      .technical-note { font-size: 0.9em; color: #666; font-style: italic; }
    "))

  ),
  tags$script(HTML("
  // when the user clicks a tab, push a new history state
  $(document).on('click', '#tabs li a', function(e){
    var tab = $(this).data('value');
    if (tab) {
      // change the URL hash (e.g. #map, #videos, #data)
      history.pushState({shinyTab: tab}, '', '#' + tab);
    }
  });

  // when the user presses Back/Forward
  window.addEventListener('popstate', function(e){
    if (e.state && e.state.shinyTab) {
      // update the Shiny input, which will switch the tab
      Shiny.setInputValue('tabs', e.state.shinyTab, {priority: 'event'});
    }
  });

  // on first load, if there’s a hash, go to that tab
  $(document).on('shiny:connected', function(){
    var initial = window.location.hash.replace('#','');
    if (initial) {
      Shiny.setInputValue('tabs', initial, {priority: 'event'});
    }
  });
")),
  titlePanel("Great Bay Particle Animation Viewer"),

  # Add introductory information panel
  div(class = "info-panel",
    h4("About the Particle Trajectory Animations"),
    p("This application displays particle trajectory animations for the Great Bay estuary system.
      The animations show how virtual particles move through the water based on hydrodynamic modeling,
      helping visualize water circulation patterns and potential pollutant or nutrient transport pathways."),

    div(class = "animation-specs",
      h5("Animation Specifications:"),
      tags$ul(
        tags$li(strong("Particle Count:"), "100 particles displayed (subsampled from 1,000 modeled particles)"),
        tags$li(strong("Time Resolution:"), "Every 10th time step shown (stride = 10)"),
        tags$li(strong("Model Resolution:"), "Based on high-resolution hydrodynamic model with 8,641 time steps"),
        tags$li(strong("Animation Types:"),
          tags$ul(
            tags$li(strong("Regular:"), "Standard particle release and tracking"),
            tags$li(strong("Alltstride:"), "Extended time series showing longer-term transport patterns")
          )
        )
      )
    ),

    p(class = "technical-note",
      "Animations were generated using particle tracking models that simulate how water-borne materials
      would move through the estuary under various environmental conditions. Each animation represents
      a specific deployment date and location within the Great Bay system.")
  ),

  sidebarLayout(
    sidebarPanel(width = 3,
      h3("Filters"),
      checkboxGroupInput("siteTypeFilter", "Site Types:",
                         choices = c("GB Sites" = "GB", "Tier2 Sites" = "tier2"),
                         selected = c("GB", "tier2")),
      uiOutput("siteCountUI"),
      hr(),
      h3("Selected Site"),
      uiOutput("selectedSiteInfo"),
      hr(),
      actionButton("clearSelection", "Clear Selection", class = "btn-warning")
    ),
    mainPanel(width = 9,
      tabsetPanel(id = "tabs",
        tabPanel(title="Map", value="map",
          leafletOutput("sitesMap", height = "600px"),
          p("Click on markers to select sites. Only sites with available particle trajectory animations are shown."),
          uiOutput("mapStatusMessage")
        ),
        tabPanel(title="Videos", value="videos",
          # Add explanatory content for the Videos tab
          div(class = "info-panel",
            h4("Particle Trajectory Animations"),
            p("The animations below show how particles released at this monitoring site would move through
              the Great Bay estuary system. Each particle represents a potential pathway for water-borne
              materials such as nutrients, pollutants, or sediments."),

            div(class = "animation-specs",
              h5("Understanding the Animations:"),
              tags$ul(
                tags$li(strong("Particle Movement:"), "Each dot represents a virtual particle moving with water currents"),
                tags$li(strong("Color Coding:"), "Particles are color-coded by site for easy identification"),
                tags$li(strong("Time Progression:"), "Animations show particle movement over time, revealing circulation patterns"),
                tags$li(strong("Multiple Dates:"), "Different animations may represent different environmental conditions")
              )
            ),

            p(strong("Scientific Applications:"),
              "These visualizations help researchers and managers understand pollutant transport,
              residence times, connectivity between sites, and potential impacts of point sources
              on water quality throughout the estuary.")
          ),

          h3("Particle Trajectory Animations"),
          div(class = "video-container", uiOutput("videoPlayerUI")),
          hr(),
          h4("Available Animations for Selected Site"),
          DTOutput("videosTable"),
          hr(),
          uiOutput("siteVariables")
        ),
        tabPanel(title="Data Summary",  value="data",
          # Add explanatory content for the Data Summary tab
          div(class = "info-panel",
            h4("Environmental Data Summary"),
            p("This section displays actual environmental monitoring data collected at the selected site.
              The data complements the particle trajectory animations by showing the real-world conditions
              that influence water movement and quality."),

            p(strong("Data Sources:"),
              "All data is retrieved from the PREP (Piscataqua Region Estuaries Partnership) database,
              which contains long-term monitoring records for water quality, eelgrass habitat, and
              other environmental parameters throughout the Great Bay estuary system.")
          ),

          h3("Environmental Monitoring Data"),
          fluidRow(
            column(12,
              uiOutput("plotStatusMessage")
            )
          ),
          hr(),
          DTOutput("variableDataTable")
        )
      )
    )
  )
)

# Server definition
server <- function(input, output, session) {
  cfg <- reactiveVal(load_configurations())
  rv  <- reactiveValues(
    selected_site = NULL,
    variable_data = NULL
  )

  # Site counts
  output$siteCountUI <- renderUI({
    df <- cfg()$sites_df
    gb_count    <- sum(df$site_type == "GB")
    tier2_count <- sum(df$site_type == "tier2")
    div(class = "site-count",
      p(paste("Total sites:", nrow(df))),
      p(paste("GB sites:", gb_count)),
      p(paste("Tier2 sites:", tier2_count))
    )
  })

  # Map status message
  output$mapStatusMessage <- renderUI({
    if (nrow(cfg()$sites_df) == 0) {
      div(class = "status-message",
        h4("No sites with particle animations found"),
        p("No sites with available particle trajectory animations were found. Please check that:"),
        tags$ul(
          tags$li("Animation files (.mp4) exist in the output/videos/ directory"),
          tags$li("Metadata file exists at output/configs/particle_animation_metadata.json"),
          tags$li("Animation generation completed successfully with the specified parameters")
        )
      )
    }
  })

  # Render map
  output$sitesMap <- renderLeaflet({
    df <- cfg()$sites_df %>% filter(!is.na(longitude) & !is.na(latitude))
    if (nrow(df) == 0) {
      return(leaflet() %>% addTiles() %>% setView(-70.85, 43.08, 11))
    }
    site_colors <- ifelse(is.na(df$color), ifelse(df$site_type == "GB", "blue", "red"), df$color)
    leaflet(df) %>%
      addTiles() %>% setView(-70.85, 43.08, 11) %>%
      addCircleMarkers(
        ~longitude, ~latitude,
        color = site_colors,
        radius = 8,
        fillOpacity = 0.8,
        stroke = FALSE,
        label = ~display_name,  # Use display_name instead of site_name
        layerId = ~site_name,  # Keep site_name as the ID for references
        group = ~site_type
      ) %>%
      addLegend(
        position = "bottomright",
        colors = c("blue", "red"),
        labels = c("GB Sites", "Tier2 Sites"),
        title = "Site Types"
      )
  })

  # Filter markers by site type
  observe({
    req(input$siteTypeFilter)
    proxy <- leafletProxy('sitesMap') %>% clearMarkers()
    df <- cfg()$sites_df %>%
      filter(site_type %in% input$siteTypeFilter &
             !is.na(longitude) & !is.na(latitude))
    if (nrow(df) > 0) {
      site_colors <- ifelse(is.na(df$color), ifelse(df$site_type == "GB", "blue", "red"), df$color)
      proxy %>% addCircleMarkers(
        data = df,
        lng = ~longitude,
        lat = ~latitude,
        color = site_colors,
        radius = 8,
        fillOpacity = 0.8,
        stroke = FALSE,
        label = ~display_name,  # Use display_name
        layerId = ~site_name,
        group = ~site_type
      )
    }
  })

  # Site click
  observeEvent(input$sitesMap_marker_click, {
    click <- input$sitesMap_marker_click; req(click$id)
    rv$selected_site <- click$id
    updateTabsetPanel(session, 'tabs', selected = 'videos')
  })

  # Reactive list of all videos for the selected site
  videos_for_site <- reactive({
    req(rv$selected_site)
    cfg()$video_info[[rv$selected_site]] %||% list()
  })

  # Display selected site info
  output$selectedSiteInfo <- renderUI({
    if (is.null(rv$selected_site)) {
      return(p("No site selected"))
    }

    site_data <- cfg()$sites_df %>% filter(site_name == rv$selected_site)
    if (nrow(site_data) == 0) {
      return(p("Site information not found"))
    }

    # Format date range info if available
    date_range_info <- ""
    if (!is.na(site_data$min_date) && !is.na(site_data$max_date)) {
      min_date_formatted <- format(as.Date(site_data$min_date), "%b %d, %Y")
      max_date_formatted <- format(as.Date(site_data$max_date), "%b %d, %Y")
      date_range_info <- paste0(
        p(strong("Data Available:"),
          paste(min_date_formatted, "to", max_date_formatted))
      )
    }

    div(class = "site-info",
      h4(site_data$display_name),
      p(strong("Site Code:"), site_data$feature_code %||% "N/A"),
      p(strong("Site Name:"), site_data$feature_name %||% "N/A"),
      p(strong("Type:"), ifelse(site_data$site_type == "GB", "Great Bay", "Tier 2")),
      p(strong("Coordinates:"), sprintf("%.6f, %.6f", site_data$latitude, site_data$longitude)),
      if (date_range_info != "") HTML(date_range_info)
    )
  })

  # Display site variables
  output$siteVariables <- renderUI({
    if (is.null(rv$selected_site)) {
      return(NULL)
    }

    vids <- videos_for_site()
    if (length(vids) == 0) return(NULL)

    # Get variables from the first video
    variables <- vids[[1]]$variables

    # Check if variables are available
    if (length(variables) == 0) {
      return(div(
        h3("Environmental Variables at Selected Site"),
        div(class = "alert alert-warning",
          p("No environmental monitoring variables were found in the database for this site."),
          p("You can try querying directly by Result ID in the Data Plot tab."),
          p("For site", strong(rv$selected_site), "try searching for data by providing a result ID from the site summaries.")
        )
      ))
    }

    # Create a table of variables with enhanced information
    var_df <- do.call(rbind, lapply(variables, function(v) {
      date_info <- paste("N/A")

      # Add date range if available
      if (!is.null(v$min_date) && !is.null(v$max_date) && !is.na(v$min_date) && !is.na(v$max_date)) {
        min_date <- substr(v$min_date, 1, 10)  # Take just the date part
        max_date <- substr(v$max_date, 1, 10)  # Take just the date part
        date_info <- paste(min_date, "to", max_date)
      }

      # Include result ID if available
      result_id <- if (!is.null(v$result_id) && !is.na(v$result_id)) v$result_id else "N/A"

      data.frame(
        Variable = v$name,
        Unit = v$unit,
        Description = v$description %||% "",
        "Result ID" = result_id,
        "Date Range" = date_info,
        stringsAsFactors = FALSE
      )
    }))

    div(
      h3("Environmental Variables at Selected Site"),
      p("The following environmental variables are monitored at this site. ",
        strong("Click a row to view the data in the Data Summary tab.")),

      # Custom styled table with a clickable body
      tags$div(
        id = "variable-table-container",
        style = "max-height: 300px; overflow-y: auto; margin-bottom: 20px;",

        # Custom table styling
        tags$style(HTML("
          #variable-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 15px;
          }
          #variable-table th {
            background-color: #f1f1f1;
            position: sticky;
            top: 0;
            z-index: 10;
          }
          #variable-table th, #variable-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
          }
          #variable-table tr:nth-child(even) {
            background-color: #f9f9f9;
          }
          #variable-table tr:hover {
            background-color: #eaf7ff;
            cursor: pointer;
          }
        ")),

        # Table with JavaScript click handler that also fetches data
        tags$table(
          id = "variable-table",
          tags$thead(
            tags$tr(
              tags$th("Variable"),
              tags$th("Unit"),
              tags$th("Result ID"),
              tags$th("Date Range")
            )
          ),
          tags$tbody(
            lapply(1:nrow(var_df), function(i) {
              # Create the onclick function that will populate and fetch data
              onclick_js <- sprintf("
                Shiny.setInputValue('selected_variable_click', {
                  result_id: '%s',
                  variable_name: '%s',
                  min_date: '%s',
                  max_date: '%s'
                });
              ",
              var_df$Result.ID[i],
              var_df$Variable[i],
              # Extract dates from the date range string
              ifelse(var_df$Date.Range[i] != "N/A",
                     strsplit(var_df$Date.Range[i], " to ")[[1]][1], ""),
              ifelse(var_df$Date.Range[i] != "N/A",
                     strsplit(var_df$Date.Range[i], " to ")[[1]][2], "")
              )

              tags$tr(
                onclick = onclick_js,
                tags$td(var_df$Variable[i]),
                tags$td(var_df$Unit[i]),
                tags$td(var_df$Result.ID[i]),
                tags$td(var_df$Date.Range[i])
              )
            })
          )
        )
      )
    )
  })

  # Clear selection
  observeEvent(input$clearSelection, {
    rv$selected_site <- NULL
    rv$variable_data <- NULL
    updateTextInput(session, "resultId", value = "")
  })

  # Handle click on variable table - enhanced to auto-fetch data
  observeEvent(input$selected_variable_click, {
variable_data <- input$selected_variable_click
  req(variable_data$result_id)

  # ← Add this:
  updateTabsetPanel(session, "tabs", selected = "data")

  withProgress(message = paste("Fetching", variable_data$variable_name, "data…"), {
    base_url   <- "http://data.prepestuaries.org:3001"
    ts_endpoint <- paste0("/timeseriesresultvalues?resultid=eq.", variable_data$result_id)
    message("GET ", ts_endpoint)
    ts_resp <- GET(paste0(base_url, ts_endpoint))
    ts_data <- fromJSON(content(ts_resp, "text", encoding="UTF-8"), simplifyVector = TRUE)

    # --- 1) check for empty data.frame
    if (!is.data.frame(ts_data) || nrow(ts_data) == 0) {
      showNotification("No time‐series data returned.", type = "warning")
      rv$variable_data <- NULL
      return()
    }

    # timestamp
    ts_data$valuedatetime <- as.POSIXct(ts_data$valuedatetime, tz = "UTC")

    # --- 2) fetch the site/variable metadata
    meta_endpoint <- paste0("/sitesummaries?resultid=eq.", variable_data$result_id)
    message("GET ", meta_endpoint)
    meta_resp   <- GET(paste0(base_url, meta_endpoint))
    meta_data   <- fromJSON(content(meta_resp, "text", encoding="UTF-8"), simplifyVector = TRUE)

    if (is.data.frame(meta_data) && nrow(meta_data) > 0) {
      site_code          <- meta_data$samplingfeaturecode[1]
      site_name          <- meta_data$samplingfeaturename[1]
      action_description <- meta_data$actiondesc[1]
      var_name           <- meta_data$variablenamecv[1]
      units              <- meta_data$unitsname[1]
      var_code           <- meta_data$variablecode[1]
    } else {
      site_code          <- NA_character_
      site_name          <- NA_character_
      action_description <- NA_character_
      var_name           <- NA_character_
      units              <- NA_character_
      var_code           <- NA_character_
    }

    # --- 3) merge everything into ts_data
    ts_data$site_code          <- site_code
    ts_data$site_name          <- site_name
    ts_data$action_description <- action_description
    ts_data$variablename       <- var_name
    ts_data$unitsname          <- units
    ts_data$variablecode       <- var_code

    rv$variable_data <- ts_data
    showNotification(
      paste("Loaded", nrow(ts_data), "points for", var_name),
      type = "message"
    )
  })
})


  # Variable selection UI that handles the case when no variables are available

  # Remove the variable selection UI since we're not using manual input anymore
  # output$variableSelectUI <- renderUI({...}) - REMOVED



  # Update variable selection when site is selected

  # Update the observer function that populates the variable dropdown
observe({
  if (is.null(rv$selected_site)) {
    updateSelectInput(session, "variableSelect", choices = character(0))
    return()
  }

  vids <- videos_for_site()
  if (length(vids) == 0) return()

  # Get variables from the first video
  variables <- vids[[1]]$variables

  # Check if variables are available
  if (length(variables) == 0) {
    updateSelectInput(session, "variableSelect", choices = character(0))
    showNotification("No variables found for this site. Try using a direct Result ID query instead.",
                   type = "warning", duration = 10)
    return()
  }

  # Create a named vector for the dropdown choices
  # In your metadata, each variable has a name, unit, code, result_id, etc.
  var_choices <- sapply(variables, function(v) {
    # Format display name with additional metadata
    display_name <- v$name

    # Add date range if available
    if (!is.null(v$min_date) && !is.null(v$max_date) &&
        !is.na(v$min_date) && !is.na(v$max_date)) {
      min_date <- substr(v$min_date, 1, 10)  # Take just the date part
      max_date <- substr(v$max_date, 1, 10)  # Take just the date part
      display_name <- paste0(display_name, " (", min_date, " to ", max_date, ")")
    }

    # Add result ID if available
    if (!is.null(v$result_id) && !is.na(v$result_id)) {
      display_name <- paste0(display_name, " [ID: ", v$result_id, "]")
    }

    return(display_name)
  })

  # Set the names of the vector to be the actual variable names
  # This is important for the selection process
  names(var_choices) <- sapply(variables, function(v) v$name)

  # Update the dropdown with our properly formatted choices
  updateSelectInput(session, "variableSelect", choices = var_choices)

  # Find the global min/max dates across all variables
  min_date <- NULL
  max_date <- NULL

  for (var in variables) {
    if (!is.null(var$min_date) && !is.na(var$min_date)) {
      var_min_date <- as.Date(substr(var$min_date, 1, 10))
      if (is.null(min_date) || var_min_date < min_date) {
        min_date <- var_min_date
      }
    }

    if (!is.null(var$max_date) && !is.na(var$max_date)) {
      var_max_date <- as.Date(substr(var$max_date, 1, 10))
      if (is.null(max_date) || var_max_date > max_date) {
        max_date <- var_max_date
      }
    }
  }


})

  # Remove the manual fetch data observer since we're auto-fetching
  # observeEvent(input$fetchData, {...}) - REMOVED

  # Display plot status message
  output$plotStatusMessage <- renderUI({
    if (is.null(rv$selected_site)) {
      div(class = "status-message",
        h4("No site selected"),
        p("Please select a site from the map to view environmental monitoring data.")
      )
    } else if (is.null(rv$variable_data)) {
      # Get site feature code to provide more helpful guidance
      site_data <- cfg()$sites_df %>% filter(site_name == rv$selected_site)
      feature_code <- if (nrow(site_data) > 0 && !is.na(site_data$feature_code))
                        site_data$feature_code else "Unknown"

      div(class = "status-message",
        h4("No environmental data loaded"),
        p("Click on an environmental variable in the Videos tab to automatically load and display the data here."),
        p("For site ", strong(rv$selected_site), " with code ", strong(feature_code),
          ", environmental monitoring data will be displayed once a variable is selected.")
      )
    } else if (nrow(rv$variable_data) == 0) {
      div(class = "status-message",
        h4("No data available"),
        p("No environmental data available for the selected variable and date range."),
        p("Try adjusting the date range or selecting a different variable.")
      )
    } else {
      div(class = "status-message",
        h4("Data loaded successfully"),
        p(paste("Displaying", nrow(rv$variable_data), "data points for",
                unique(rv$variable_data$variablename)[1], "at site", rv$selected_site))
      )
    }
  })

  # Render enhanced variable data table with site code and action description
  output$variableDataTable <- renderDT({
    req(rv$variable_data)
    if (nrow(rv$variable_data) == 0) return(NULL)

    # Format the data for display with additional metadata
    display_data <- rv$variable_data %>%
      mutate(
        # Format datetime to be more readable
        formatted_date = format(valuedatetime, "%Y-%m-%d"),
        formatted_time = format(valuedatetime, "%H:%M:%S")
      ) %>%
      select(
        "Date" = formatted_date,
        "Time" = formatted_time,
        "Value" = datavalue,
        "Variable" = variablename,
        "Units" = unitsname,
        "Site Code" = site_code,
        "Site Name" = site_name,
        "Action Description" = action_description,
        "Result ID" = resultid
      )

    # Create the data table with additional options
    datatable(
      display_data,
      options = list(
        pageLength = 15,
        scrollX = TRUE,
        dom = 'Bfrtip',  # Add buttons to the table
        buttons = c('copy', 'csv', 'excel'),  # Allow export options
        searchHighlight = TRUE,  # Highlight search matches
        columnDefs = list(list(width = '100px', targets = c(0, 1)),  # Date and Time columns
                         list(width = '80px', targets = 2),   # Value column
                         list(width = '120px', targets = 3),  # Variable column
                         list(width = '60px', targets = 4),   # Units column
                         list(width = '100px', targets = 5),  # Site Code column
                         list(width = '150px', targets = 6),  # Site Name column
                         list(width = '200px', targets = 7))  # Action Description column
      ),
      rownames = FALSE,
      filter = 'top',  # Add filters to each column
      extension = 'Buttons',  # Enable buttons extension
      caption = paste("Environmental monitoring data for",
                     if(nrow(display_data) > 0) unique(display_data$Variable)[1] else "selected variable",
                     "- Total records:", nrow(display_data))
    ) %>%
    formatStyle(
      'Value',
      background = styleColorBar(range(display_data$Value, na.rm = TRUE), 'lightblue'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'right'
    )
  })

  # Remove the plot rendering function since we're only showing the table
  # output$variablePlot <- renderPlotly({...}) - REMOVED

  # Videos table: show all with display names and enhanced descriptions
  output$videosTable <- renderDT({
    vids <- videos_for_site()
    if (length(vids) == 0) return(NULL)
    df <- do.call(rbind, lapply(vids, function(v) {
      # Enhanced type description
      type_desc <- if (v$is_alltstride) {
        "Extended Timeline (Alltstride)"
      } else {
        "Standard Release"
      }

      # Add particle info
      particle_info <- "100 particles (1:10 subsampled)"

      data.frame(
        Animation = v$filename,
        DisplayName = v$display_name %||% "Unknown",
        Date = as.character(v$date),
        Type = type_desc,
        Particles = particle_info,
        stringsAsFactors = FALSE
      )
    }))

    datatable(df,
              options = list(pageLength = 5),
              caption = "Available particle trajectory animations for this monitoring site.
                        Each animation shows 100 particles (subsampled from 1,000 modeled particles)
                        with every 10th time step displayed.")
  })

  # Video players: one per file with enhanced descriptions
  output$videoPlayerUI <- renderUI({
    vids <- videos_for_site()
    if (length(vids) == 0) {
      return(div(
        class = "status-message",
        h4("No particle trajectory animations available"),
        p("No particle trajectory animations are available for this site."),
        p("Animations show how particles released at this location would move through the Great Bay estuary system.")
      ))
    }

    players <- lapply(vids, function(v) {
      display_name <- v$display_name %||% v$filename
      # Clean up date display
      date_display <- if(v$date != "unknown") paste("(", v$date, ")") else ""
      # Add alltstride indicator if needed
      alltstride_text <- if(v$is_alltstride) " - Extended Timeline" else " - Standard Release"

      # Create description based on animation type
      description <- if (v$is_alltstride) {
        "Extended particle tracking showing longer-term transport patterns and residence times."
      } else {
        "Standard particle release showing initial dispersion and short-term transport pathways."
      }

      tags$div(
        style = "margin-bottom: 30px; border: 1px solid #ddd; border-radius: 8px; padding: 15px;",
        h4(paste0(display_name, date_display, alltstride_text)),
        p(class = "technical-note", description),
        p(class = "technical-note",
          "Animation shows 100 particles (subsampled from 1,000 modeled particles) with every 10th time step displayed."),
        tags$video(
          src = file.path("videos", v$filename),
          type = "video/mp4",
          controls = TRUE,
          style = "width:100%; border-radius: 4px;"
        )
      )
    })
    do.call(tagList, players)
  })
}

# Run app
shinyApp(ui, server)