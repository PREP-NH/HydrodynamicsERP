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
ui <- fluidPage(
    tags$head(
        tags$style(HTML("
            .shiny-text-output, .shiny-verbatim-text-output {
                text-align: left;
                margin-top: 0px;
                margin-bottom: 0px;
            }
            img {
                width: 100%;
                height: auto;
                display: block;
                margin-bottom: 0px;
            }
            .shiny-output-container {
                padding: 0px;
            }
        "))
    ),
    titlePanel("Water Depth and Velocity with the Changing Tides"),

    # Checkbox for switching view
    checkboxInput("detailedView", "Switch to Detailed View", value = FALSE),

    # Conditional panel for combinedSlider
    conditionalPanel(
        condition = "!input.detailedView",
        HTML("<p>Select an index: High Tide (76), Low Tide (101), High Tide (125), Low Tide (200),
                       High Tide (277), Low Tide (299), Max Velocity (311),
                      High Tide (323), Low Tide (398), Max Velocity (410),
                      High Tide (423)</p>"),
        sliderTextInput("combinedSlider", "Time:", width = '100%',
                        choices = c(76,101,125, 200, 277,
                                    299, 311, 323, 398, 410, 423),
                        selected = 76)
    ),

    # Conditional panel for timeSlide
    conditionalPanel(
        condition = "input.detailedView",
        sliderInput("timeSlide", "Time:", min = 1, max = 500, value = 1, width = '100%')
    ),

    fluidRow(
        column(12,
            textOutput("fileFeedback"),
            verbatimTextOutput("vectorLengthsOutput"),
            uiOutput("map2")
        )
    )
)

server <- function(input, output, session) {
    selectedSlide <- reactive({
        if(input$detailedView) {
            req(input$timeSlide)
            input$timeSlide
        } else {
            req(input$combinedSlider)
            input$combinedSlider
        }
    })

    mapResults <- reactive({
        tryCatch({
            make_map(selectedSlide())  # Call the Python function using the selected slider value
        }, error = function(e) {
            message("Error in make_map: ", e$message)
            return(NULL)
        })
    })

    output$map2 <- renderUI({
        res <- mapResults()  # Retrieve the results
        if (is.null(res)) {
            return(NULL)
        }

        map_file_path <- res[[1]]
        message("Map generated: ", map_file_path)
        img(src = map_file_path)  # Display the map image
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

shinyApp(ui, server)

# ... [Rest of the Code]
