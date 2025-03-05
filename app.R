# library("ncdf4")
library("lubridate")
library("tidyverse")
# library("rasterVis")
library("raster")
# library("proj4")
library("jsonlite")
library("shinyWidgets")
# nc <- nc_open("gbe_0001_his_vars_hays_5days_spring_tide_whole_gb.nc")

# Load necessary libraries
library(shiny)
# library(leaflet)







# File index 76: Max Tide Depth = 0.6265354
# File index 101: Min Tide Depth = -1.3039455
# File index 150: Max Tide Depth = 0.6266422
# File index 200: Min Tide Depth = -1.313385
# File index 224: Min Velocity = 0.26665163
# File index 277: Max Tide Depth = 0.6266422
# File index 299: Min Tide Depth = -1.2998047
# File index 311: Max Velocity = 1.1561589
# File index 323: Min Velocity = 0.26817834
# File index 348: Max Tide Depth = 0.6266422
# File index 398: Min Tide Depth = -1.264883
# File index 410: Max Velocity = 1.1430843
# File index 423: Max Tide Depth = 0.6266918
# youtube embeds
# https://youtu.be/TioPn3x1GYo
# https://youtu.be/P_lXrMkJeWQ
# Shiny UI
# Other libraries you might need...
# Define your UI
# Define your UI
ui <- fluidPage(
    tags$head(
        tags$style(HTML("
            .loading-screen {
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-color: rgba(255, 255, 255, 0.8);
                z-index: 10000;
                display: flex;
                justify-content: center;
                align-items: center;
                font-size: 2em;
            }
        "))
    ),

    div(class = "loading-screen", "Loading..."),
    titlePanel("Water Depth and Velocity with the Changing Tides"),

    # Checkbox for switching view
    checkboxInput("detailedView", "Switch to Detailed View", value = FALSE),

    # Checkboxes and slider in the detailed view
    conditionalPanel(
        condition = "input.detailedView",
        fluidRow(
            column(2,
                   checkboxInput("loopSlider", "Advance in time automatically", value = FALSE),
                   helpText("Hint: if automatic playback is not smooth, let the looping start over after it completes one cycle, or start and stop looping.")
            ),
            column(10, sliderInput("timeSlide", "Time:", min = 1, max = 500, value = 1, width = '100%'))
        )
    ),

    # Breadcrumbs for combined view
    conditionalPanel(
        condition = "!input.detailedView",
        uiOutput("breadcrumbLinks")
    ),

    fluidRow(
        column(12,
               verbatimTextOutput("vectorLengthsOutput"),
               uiOutput("map2")
        )
    ),

    # Hidden div for preloading images
    tags$div(style = "display: none;",
        lapply(1:500, function(i) {
            img(src = paste0("/mnt/shiny/hydrodynamics/maps/my_map", i, ".png"), id = paste0("image", i))
        })
    )
)

getFilePaths <- function(time_slide) {
  if (file.exists('/mnt/shiny/hydrodynamics/maps/')) {
    base_path <- '/mnt/shiny/hydrodynamics/maps/'
  } else {
    base_path <- './maps/'
  }

  mapfile <- paste0(base_path, 'my_map', time_slide, '.png')
  data_file <- paste0(base_path, 'map_data_', time_slide, '.txt')

  list(mapfile = mapfile, data_file = data_file)
}

# Define your server logic
server <- function(input, output, session) {
    selectedBreadcrumb <- reactiveVal(76)
    autoAdvance <- reactiveTimer(700)  # 0.4 seconds

    # Render breadcrumb links
    output$breadcrumbLinks <- renderUI({
        breadcrumbChoices <- c("High Tide: Friday 2019-8-30 5:30 AM ->" = 76, "Low Tide Friday 11:45 AM ->" = 101, "High Tide Friday 6 PM ->" = 126,
                               "Low Tide: Saturday 8-31 12:45 AM ->" = 201, "High Tide Saturday 6:45 PM ->" = 225, "Low Tide Sunday: 9-1 1 AM" = 250,
                               "-- Max Velocity into Great Bay Sunday: 9-1 4:15 PM" = 311,  "-- Max Velocity out of Great Bay Sunday: 9-1 11 PM" = 338)

        do.call(tagList, lapply(names(breadcrumbChoices), function(name) {
            a(href = "#", name,
              onclick = sprintf("Shiny.setInputValue('selectedBreadcrumb', %d); return false;", breadcrumbChoices[[name]]))
        }))
    })

    # Observe breadcrumb selection
    observeEvent(input$selectedBreadcrumb, {
        selectedBreadcrumb(input$selectedBreadcrumb)
    })

    # Initial map and vector lengths output
    initialIndex <- as.character(76)
    initialResults <- getFilePaths(initialIndex)

    # Display initial map and vector lengths output
    output$map2 <- renderUI({
        img(src = initialResults[[1]])  # Display the initial map image
    })

     loopObserver <- reactiveVal(NULL)


    # Modify the observer for the slider to display the preloaded image
    observeEvent(input$timeSlide, {
        if(input$detailedView) {
            # Construct the image path
            image_path <- paste0("/mnt/shiny/hydrodynamics/maps/my_map", input$timeSlide, ".png")

            # Update the UI to show the current image
            output$map2 <- renderUI({
                img(src = image_path)
            })
        }
    })

    # Observe the loop checkbox for changes
     # Observe the loop checkbox for changes
    observeEvent(input$loopSlider, {
    if (input$loopSlider) {
        # If loop is enabled, start the loop to update the slider value
        loopObserver(observe({
            autoAdvance()  # Reactive dependency on the timer
            currentVal <- input$timeSlide
            if (currentVal < 500) {
                # Delay might be added here if needed
                Sys.sleep(0.01)  # Adjust the sleep time as necessary
                updateSliderInput(session, "timeSlide", value = currentVal + 1)
            } else {
                updateSliderInput(session, "timeSlide", value = 1)  # Reset to start
            }
        }))
    } else {
        # If loop is disabled, stop the loop
        isolate({
            if (!is.null(loopObserver())) {
                loopObserver()$destroy()  # Stop the observer
                loopObserver(NULL)
            }
        })
    }
})

    output$vectorLengthsOutput <- renderText({
        res <- mapResults()  # Retrieve the results
        if (is.null(res)) {
            return("Data file not found.")
        }

        data_file_path <- res[[2]]
        message("Data file generated: ", data_file_path)
        if (file.exists(data_file_path)) {
            data <- fromJSON(data_file_path)
            outtext <- paste("Range of velocities in meters per second: ", data$vector_lengths_range)
            outtext <- paste(outtext,"\nRange of depths in meters: ", data$depth_range)
            paste(outtext,"\nBased on modeled hydrodynamics from: ", data$date_and_time)

        } else {
            "Data file not found."
        }
    })
    # Reactive expression for map results
    mapResults <- reactive({
        # Determine the appropriate index to use
        slideIndex <- if(input$detailedView) {
            as.integer(req(input$timeSlide))  # Convert to integer
        } else {
            as.integer(req(selectedBreadcrumb()))  # Convert to integer
        }

        # Call the Python function using the determined slider value
        tryCatch({
            getFilePaths(slideIndex)
        }, error = function(e) {
            message("Error in getFilePaths: ", e$message)
            return(NULL)
        })
    })
    # Observe changes in mapResults and update UI accordingly
    observe({
        res <- mapResults()
        if (!is.null(res)) {
            output$map2 <- renderUI({
                img(src = res[[1]])  # Display the map image
            })
          output$vectorLengthsOutput <- renderText({
            res <- mapResults()  # Retrieve the results
            if (is.null(res)) {
                return("Data file not found.")
            }

            data_file_path <- res[[2]]
            message("Data file generated: ", data_file_path)
            if (file.exists(data_file_path)) {
                data <- fromJSON(data_file_path)
                outtext <- paste("Range of velocities in meters per second: ", data$vector_lengths_range)
                outtext <- paste(outtext,"\nRange of depths in meters: ", data$depth_range)
                paste(outtext,"\nBased on modeled hydrodynamics from: ", data$date_and_time)

            } else {
                "Data file not found."
            }
        })
        }
    })
    observe({
        removeUI(selector = ".loading-screen")
    })
}

# Run the application
shinyApp(ui, server)
