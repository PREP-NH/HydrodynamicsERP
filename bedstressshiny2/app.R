library(shiny)

# Define UI
ui <- fluidPage(
    titlePanel(h1("Bed Shear Stress Maps", align = "center")),

    fluidPage(
        tags$style(type="text/css",
                   ".selectize-input { width: 200% !important; }",
                   ".shiny-input-container { margin: auto; width: 50%; }",
                   "img { width: 160% !important; }"),  # Add this line to adjust image size
        mainPanel(
           selectInput("mapType", "Select Map Type:",
                        choices = c("Average bed shear stress over a 30 day period in Newtons per square meter" = "bsmean",
                                    "RMS of bed shear stress over a 30 day period in Newtons per square meter" = "bsrms",
                                    "Average of the highest 1/3 bed shear stress values over a 30 day period in Newtons per square meter" = "bsonethird",
                                    "Average of the highest 1/10 bed shear stress values over a 30 day period in Newtons per square meter" = "bsonetenth",
                                    "Duration in hours that bed shear stress exceeds critical value of 0.35 Newtons per meter squared over a 35 day period (hours)" = "bsdur",
                                    "Fraction time bed shear stress exceeds 0.35 Newtons per square meter over a 35 day period" = "bsfrbdur")),
           imageOutput("mapImage")
        )
    )
)

# Define server logic
server <- function(input, output) {
    # Render the image
    output$mapImage <- renderImage({
        # Dynamically generate the path to the map file
        mapfile <- paste0('./maps/my_', input$mapType, '.png')

        # Return the image information
        list(src = mapfile,
             contentType = 'image/png',
             alt = paste("Map for", input$mapType))
    }, deleteFile = FALSE)
}

# Run the application
shinyApp(ui = ui, server = server)
