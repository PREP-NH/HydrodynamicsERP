# app.R - Great Bay Particle Animation Viewer (Updated: always show all videos)

library(shiny)
library(leaflet)
library(dplyr)
library(jsonlite)
library(shinyjs)
library(shinyWidgets)
library(DT)
library(stringr)

# Setup resource paths
addResourcePath("videos", "output/videos")

# Helper functions
`%||%` <- function(a, b) if (is.null(a)) b else a
safe_get <- function(x, name) if (is.list(x) && name %in% names(x)) x[[name]] else NULL

# Load configurations (unchanged)
load_configurations <- function() {
  tryCatch({
    particle_path <- "output/configs/particle_animation_metadata.json"
    if (!file.exists(particle_path)) {
      warning("Metadata file not found at: ", particle_path)
      return(list(sites_df = data.frame(), video_info = list()))
    }
    pdata <- fromJSON(particle_path, simplifyVector = FALSE)
    files_list <- safe_get(pdata, "files") %||% list()

    # Build sites_df
    sites_df <- tibble::tibble(
      site_name = character(),
      site_type = character(),
      longitude = double(),
      latitude = double(),
      color = character()
    )
    processed_sites <- character()

    for (item in files_list) {
      site_name <- safe_get(item, "site")
      if (is.null(site_name) || site_name == "" || site_name %in% processed_sites) next
      lon_range <- safe_get(item, "lon_range")
      lat_range <- safe_get(item, "lat_range")
      color     <- safe_get(item, "color") %||% NA_character_
      site_type <- ifelse(grepl("^GB", site_name), "GB", "tier2")
      lon <- if (is.list(lon_range) && length(lon_range)>0) unlist(lon_range)[1] else NA_real_
      lat <- if (is.list(lat_range) && length(lat_range)>0) unlist(lat_range)[1] else NA_real_
      sites_df <- dplyr::add_row(
        sites_df,
        site_name = site_name,
        site_type = site_type,
        longitude = lon,
        latitude  = lat,
        color     = color
      )
      processed_sites <- c(processed_sites, site_name)
    }

    # Build video_info
    video_info <- list()
    for (item in files_list) {
      site_name <- safe_get(item, "site")
      if (is.null(site_name) || site_name == "") next
      filename <- safe_get(item, "filename")
      date     <- safe_get(item, "date") %||% "unknown"
      is_alltstride <- safe_get(item, "is_alltstride") %||% FALSE
      video_info[[site_name]] <- c(video_info[[site_name]],
        list(list(
          filename     = filename,
          date         = date,
          is_alltstride= is_alltstride,
          metadata     = item
        ))
      )
    }

    list(sites_df = sites_df, video_info = video_info)
  }, error = function(e) {
    message("Error loading configurations: ", e$message)
    list(sites_df = tibble::tibble(), video_info = list())
  })
}

# UI
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
    "))
  ),
  titlePanel("Great Bay Particle Animation Viewer"),
  sidebarLayout(
    sidebarPanel(width = 3,
      h3("Filters"),
      checkboxGroupInput("siteTypeFilter", "Site Types:",
                         choices = c("GB Sites" = "GB", "Tier2 Sites" = "tier2"),
                         selected = c("GB", "tier2")),
      uiOutput("siteCountUI"),
      hr(),
      h3("Selected Site"),
      verbatimTextOutput("selectedSiteText"),
      hr(),
      actionButton("clearSelection", "Clear Selection", class = "btn-warning")
    ),
    mainPanel(width = 9,
      tabsetPanel(id = "tabs",
        tabPanel("Map",
          leafletOutput("sitesMap", height = "600px"),
          p("Click on markers to select sites. Only sites with available videos are shown."),
          uiOutput("mapStatusMessage")
        ),
        tabPanel("Videos",
          h3("Site Videos"),
          div(class = "video-container", uiOutput("videoPlayerUI")),
          hr(),
          h4("Available Videos for Selected Site"),
          DTOutput("videosTable")
        )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  cfg <- reactiveVal(load_configurations())
  rv  <- reactiveValues(selected_site = NULL)

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
        h4("No sites with videos found"),
        p("No sites with available videos were found. Please check that:"),
        tags$ul(
          tags$li("Video files (.mp4) exist in the output/videos/ directory"),
          tags$li("Metadata file exists at output/configs/particle_animation_metadata.json")
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
        color     = site_colors,
        radius    = 8,
        fillOpacity = 0.8,
        stroke      = FALSE,
        label       = ~site_name,
        layerId     = ~site_name,
        group       = ~site_type
      ) %>%
      addLegend(
        position = "bottomright",
        colors   = c("blue", "red"),
        labels   = c("GB Sites", "Tier2 Sites"),
        title    = "Site Types"
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
        lng  = ~longitude,
        lat  = ~latitude,
        color     = site_colors,
        radius    = 8,
        fillOpacity = 0.8,
        stroke      = FALSE,
        label       = ~site_name,
        layerId     = ~site_name,
        group       = ~site_type
      )
    }
  })

  # Site click
  observeEvent(input$sitesMap_marker_click, {
    click <- input$sitesMap_marker_click; req(click$id)
    rv$selected_site <- click$id
    updateTabsetPanel(session, 'tabs', selected = 'Videos')
  })

  # Reactive list of all videos for the selected site
  videos_for_site <- reactive({
    req(rv$selected_site)
    cfg()$video_info[[rv$selected_site]] %||% list()
  })

  # Display selected site text
  output$selectedSiteText <- renderText({
    rv$selected_site %||% "No site selected"
  })

  # Clear selection
  observeEvent(input$clearSelection, {
    rv$selected_site <- NULL
  })

  # Videos table: show all
  output$videosTable <- renderDT({
    vids <- videos_for_site()
    if (length(vids) == 0) return(NULL)
    df <- do.call(rbind, lapply(vids, function(v) {
      data.frame(
        Video = v$filename,
        Date  = as.character(v$date),
        Type  = ifelse(v$is_alltstride, "Alltstride", "Regular"),
        stringsAsFactors = FALSE
      )
    }))
    datatable(df, options = list(pageLength = 5))
  })

  # Video players: one per file
  output$videoPlayerUI <- renderUI({
    vids <- videos_for_site()
    if (length(vids) == 0) return(p("No videos available for this site."))
    players <- lapply(vids, function(v) {
      tags$div(
        h4(paste(v$filename, "(", v$date,
                 if (v$is_alltstride) "/ Alltstride" else "", ")")),
        tags$video(
          src    = file.path("videos", v$filename),
          type   = "video/mp4",
          controls = TRUE,
          style = "width:100%; margin-bottom:20px;"
        )
      )
    })
    do.call(tagList, players)
  })
}

# Run app
shinyApp(ui, server)
