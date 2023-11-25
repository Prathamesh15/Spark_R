library(devtools)
install_github(repo = "rstudio/spark-install",subdir = "R")
library(sparkinstall)
spark_available_versions()
spark_install(version = "2.4.3")
spark_installed_versions()

install.packages("sparklyr",dependencies = T)

library(sparklyr)

config <- spark_config()

config$'sparklyr.cores.local' = 6

config$'sparklyr.shell.driver-memory' = "16G"

config$'spark.memory.fraction' = 0.9

Sys.setenv(JAVA_HOME = "C:\\Users\\thite\\java_8\\jdk")

sc <- spark_connect(master = "local", version = "2.4.3", config =  config)

# Load sample iris data into a Spark DataFrame
iris_tbl <- sdf_copy_to(sc, iris, "iris", overwrite = TRUE)

# Show the first few rows of the Spark DataFrame
head(iris_tbl)
# Assuming your data frame is named parquet_tbl
# Install and load necessary packages

# Specify the path to your CSV file
csv_path <- "C:\\Users\\thite\\Downloads\\HPI_AT_BDL_ZIP5.csv"

# Load CSV data into a Spark DataFrame
csv_tbl <- spark_read_csv(sc, path = csv_path)

# Show the first few rows of the CSV Spark DataFrame
head(csv_tbl)

names(csv_tbl)
# Group by Year and calculate average HPI
# Group by Year, filter out null values, and calculate average HPI
# Specify the problematic year to exclude
problematic_year <- 1981  # Update with the actual problematic year

# Group by Year, filter out null values and the problematic year, and calculate average HPI
average_hpi <- csv_tbl %>%
  filter(!is.null(HPI) & !is.na(HPI) & Year != problematic_year) %>%
  distinct(Year, .keep_all = TRUE) %>%
  group_by(Year) %>%
  summarise(Avg_HPI = mean(HPI, na.rm = TRUE)) %>%
  arrange(Year) %>%
  collect()

# Plot the line chart
plot(average_hpi$Year, average_hpi$Avg_HPI, type = "l", 
     main = "Average HPI Over Years",
     xlab = "Year", ylab = "Average HPI")






# Descriptive statistics
summary_stats <- csv_tbl %>%
  summarise(
    mean_Annual_Change = mean(Annual_Change_, na.rm = TRUE),
    min_Annual_Change = min(Annual_Change_, na.rm = TRUE),
    max_Annual_Change = max(Annual_Change_, na.rm = TRUE),
    mean_HPI = mean(HPI, na.rm = TRUE),
    min_HPI = min(HPI, na.rm = TRUE),
    max_HPI = max(HPI, na.rm = TRUE),
    mean_HPI_1990 = mean(HPI_with_1990_base, na.rm = TRUE),
    min_HPI_1990 = min(HPI_with_1990_base, na.rm = TRUE),
    max_HPI_1990 = max(HPI_with_1990_base, na.rm = TRUE),
    mean_HPI_2000 = mean(HPI_with_2000_base, na.rm = TRUE),
    min_HPI_2000 = min(HPI_with_2000_base, na.rm = TRUE),
    max_HPI_2000 = max(HPI_with_2000_base, na.rm = TRUE)
  ) %>%
  collect()

# Print summary statistics
print(summary_stats)
summary_stats



lot2)

# Columns of interest
columns_of_interest <- c("Annual_Change_", "HPI", "HPI_with_1990_base", "HPI_with_2000_base")

# Descriptive statistics for selected columns
summary_stats <- csv_tbl %>%
  summarise(across(all_of(columns_of_interest), list(mean = ~mean(., na.rm = TRUE),
                                                     min = ~min(., na.rm = TRUE),
                                                     max = ~max(., na.rm = TRUE)))) %>%
  collect()

# Convert the summary_stats Spark DataFrame to a long format
summary_stats_long <- tidyr::gather(summary_stats, key = "Statistic", value = "Value")
summary_stats_long



# Assuming your Spark DataFrame is named csv_tbl and you have already loaded the sparklyr package

# Install and load necessary packages
install.packages("sparklyr")
library(sparklyr)

# Assuming your Spark DataFrame is named csv_tbl and you have already loaded the sparklyr package

# Install and load necessary packages
install.packages("sparklyr")
library(sparklyr)

# Filter out null values for each base year
hpi_1990 <- csv_tbl %>% filter(!is.na(HPI_with_1990_base)) %>% select(HPI_with_1990_base)
hpi_2000 <- csv_tbl %>% filter(!is.na(HPI_with_2000_base)) %>% select(HPI_with_2000_base)

# Calculate summary statistics for each base year
summary_stats_1990 <- hpi_1990 %>% summarise(
  mean = mean(HPI_with_1990_base, na.rm = TRUE),
  sd = sd(HPI_with_1990_base, na.rm = TRUE)
) %>% collect()

summary_stats_2000 <- hpi_2000 %>% summarise(
  mean = mean(HPI_with_2000_base, na.rm = TRUE),
  sd = sd(HPI_with_2000_base, na.rm = TRUE)
) %>% collect()

# Combine summary statistics into a single data frame
comparison_summary <- bind_rows(
  mutate(summary_stats_1990, Base_Year = 1990),
  mutate(summary_stats_2000, Base_Year = 2000)
)

# Print the comparison summary
print(comparison_summary)




# Convert Spark DataFrame to R DataFrame
csv_tbl_local <- collect(csv_tbl)

# Assuming you have a column "Year" and "HPI" in your data
average_hpi <- csv_tbl_local %>%
  filter(!is.na(Year) & !is.na(HPI)) %>%
  group_by(Year) %>%
  summarise(Avg_HPI = mean(HPI, na.rm = TRUE))

# Assuming your last known year is the maximum year in the dataset
last_known_year <- max(average_hpi$Year)

# Filter data for the last known year
last_year_data <- csv_tbl_local %>%
  filter(Year == last_known_year)

# Calculate the average change in HPI from the historical data
average_change <- mean(diff(average_hpi$Avg_HPI))

# Predict the HPI for the next year
predicted_hpi <- last_year_data$HPI + average_change

# Print the predicted HPI
print(predicted_hpi)

# Convert HPI column to numeric
time_series_data$HPI <- as.numeric(time_series_data$HPI)

