# library("ncdf4")
library("lubridate")
library("tidyverse")
# library("rasterVis")
library("raster")
# library("proj4")
library("reticulate")
library("jsonlite")
library("shinyWidgets")
# nc <- nc_open("gbe_0001_his_vars_hays_5days_spring_tide_whole_gb.nc")

# Load necessary libraries
library(shiny)
# library(leaflet)
tryCatch({
    use_python("/mnt/envs/odm2adminenv/bin/python", required = TRUE)
    message("Using production Python environment.")
}, error = function(e) {
    # If the production environment is not available, use the test environment
    message("Production Python environment not found. Switching to test environment.")
    # use_python("/home/miguelcleon/miniconda3/envs/czmanager3/bin/python", required = TRUE)
    use_python("C:/Users/ml1451/AppData/Local/Programs/Python/Python310/python.exe", required = TRUE)
})
source_python("bedshearmaponly.py")






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
ui <- fluidPage(
    titlePanel("Water Depth and Velocity with the Changing Tides"),

    # Checkbox for switching view
    checkboxInput("detailedView", "Switch to Detailed View", value = FALSE),

    # Breadcrumbs for combined view
    conditionalPanel(
        condition = "!input.detailedView",
        uiOutput("breadcrumbLinks")
    ),

    # Slider for detailed view
    conditionalPanel(
        condition = "input.detailedView",
        sliderInput("timeSlide", "Time:", min = 1, max = 500, value = 1, width = '100%')
    ),

    fluidRow(
        column(12,
               verbatimTextOutput("vectorLengthsOutput"),
                  uiOutput("map2")

        )
    )
)

# Define your server logic
server <- function(input, output, session) {
    selectedBreadcrumb <- reactiveVal(76)

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
    initialResults <- make_map(initialIndex)

    # Display initial map and vector lengths output
    output$map2 <- renderUI({
        img(src = initialResults[[1]])  # Display the initial map image
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
            make_map(slideIndex)
        }, error = function(e) {
            message("Error in make_map: ", e$message)
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
}

# Run the application
shinyApp(ui, server)
