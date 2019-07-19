## Data importing and cleaning script

library(readr)
library(dplyr)
library(lubridate)
library(ggmap)
library(stringr)
library(tidyr)
library(purrr)

## Reading in the resale flat prices data

resaledata_filenames <- Sys.glob("Data/resale-flat-prices*.csv") # obtain all filenames within the folder
data <- list()
for (i in 1:length(resaledata_filenames)) { # iterating over each file, removing an extra column
  df <- read_csv(resaledata_filenames[i])
  if ("remaining_lease" %in% colnames(df)) {
    df <- df %>%
      select(-remaining_lease)
  }
  data[[i]] <- df
}
resaledata_raw <- bind_rows(data) # binding all the files together

## Reading in Singapore's annual inflation data

inflation <- read_csv("Data/Singapore Inflation (Annual).csv")

## Cleaning the resale flat prices data

resaledata <- resaledata_raw %>%
  mutate(transaction_month = as.Date(paste(month, "01", sep = "-")),
         flat_age = year(transaction_month) - lease_commence_date,
         adj_resale_price = resale_price*(100/inflation$CPI[match(year(transaction_month), inflation$Year)]), # adjusting the nominal prices
         address = paste(block, street_name, "Singapore"),
         flat_type = str_replace(flat_type, "-", " "),
         flat_type = str_to_title(flat_type),
         street_name = str_to_title(street_name),
         town = str_to_title(town)) %>% # preparation for geocoding
  filter(transaction_month < as.Date("2019-01-01"),
         town != "Lim Chu Kang") %>%
  select(-month)

write_csv(resaledata, "resaledata.csv")

## Reading in Singapore's quarterly GDP data

GDP <- read_csv("Data/Singapore GDP (Quarterly).csv", skip = 4) %>%
  select(-X3) %>%
  rename(real_GDP = `GDP In Chained (2015) Dollars`,
         Quarter = Time) %>%
  filter(!is.na(real_GDP)) %>%
  separate(Quarter, c('Year', 'Quarter'), sep = "\\s") %>%
  mutate(real_GDP_change = ((real_GDP - lag(real_GDP, 4))/real_GDP * 100)) %>%
  filter(Year >= 1990, Year < 2019) 

write_csv(GDP, "sg_gdp.csv")

## Singapore resident information

residents <- read_csv("Data/singapore-residents-by-ethnic-group-and-sex-end-june-annual.csv") %>%
  filter(level_1 == 'Total Residents') %>%
  rename(num_residents = value) %>%
  select(-level_1) %>%
  mutate(num_residents_change = (num_residents - lag(num_residents, 1)),
         num_residents_percent = round(((num_residents - lag(num_residents, 1))/num_residents * 100), 2)) %>%
  filter(year >= 1990, year < 2019)

write_csv(residents, "sg_residents.csv")

### Finding out absolute change in number of residents between 1999 and 2008
residents$num_residents[residents$year == 2008] - residents$num_residents[residents$year == 1999]

### Finding out percentage change in number of residents between 1999 and 2008
412978/residents$num_residents[residents$year == 1999]

## HDB flats constructed information

hdb_flats_built <- read_csv("Data/flats-constructed-by-housing-and-development-board-annual.csv")

### Finding out how many HDB flats were constructed between 1999 and 2008
sum(hdb_flats_built$flats_constructed[hdb_flats_built$year %in% 1999:2008])

## Obtain the coordinates of each HDB flat in Singapore that had been transacted

#addresses_togeocode <- resaledata %>% 
#  distinct(address, town) %>%
#  mutate_geocode(address) # this is the fastest way to geocode the data, but for some reason this doesn't work for me

for (i in 1:89) { # this is a slower approach (doing 100 at a time) but is better at tracking progress
  start <- ((i-1)*100) + 1
  end <- start + 99
  addresses_togeocode[start:end, 3:4] <- suppressMessages(geocode(addresses_togeocode$address[start:end], output = "latlon"))
  print(paste(start, "to", end, "completed"))
  print(Sys.time())
}
addresses_togeocode[8901:8904, 3:4] <- suppressMessages(geocode(addresses_togeocode$address[8901:8904], output = "latlon"))

incomplete_addresses <- addresses_togeocode %>% # not all addresses were geocoded
  filter(is.na(lat) | is.na(lon)) %>%
  select(-lat, -lon) %>%
  mutate(address2 = str_replace(address, "Block", ""), # in retrospect, adding Block was unnecessary and raised some problems
         address2 = str_replace(address2, "C'WEALTH", "COMMONWEALTH")) %>%
  mutate_geocode(address2, output = "latlon") %>%
  select(-address2)

incorrect_addresses <- addresses_togeocode %>% # one address was geocoded incorrectly (to Jakarta)
  filter(lat < 0 | lat > 2 | lon < 102 | lon > 108) %>% # checks for any serious error in geocodinng
  select(-lat, -lon) %>%
  mutate(address2 = paste(address, town),
         address2 = str_replace(address2, "Block", "")) %>%
  mutate_geocode(address2, output = "latlon") %>%
  select(-address2)

addresses_geocoded <- addresses_togeocode %>% # binds all the results together into the final dataframe
  filter(!(lat < 0 | lat > 2 | lon < 102 | lon > 108)) %>%
  filter(!is.na(lat) & !is.na(lon)) %>%
  bind_rows(incomplete_addresses) %>%
  bind_rows(incorrect_addresses) %>%
  arrange(town)

## Obtain the coordinates of the remaining HDB flats that were not transacted

hdb_info_raw <- read_csv("Data/hdb-property-information.csv") %>%
  mutate(address = paste(blk_no, street, "Singapore"))

hdb_info_missing <- hdb_info_raw %>%
  mutate(address = paste("Block", blk_no, street, "Singapore")) %>% # for purposes of removing addresses that were already geocoded
  anti_join(addresses_geocoded) %>%
  mutate(address = str_replace(address, "Block ", ""),
         address = str_replace(address, "C'WEALTH", "COMMONWEALTH")) %>% # preventive
  select(address, bldg_contract_town)

for (i in 34) {
  start <- ((i-1)*100) + 1
  end <- start + 99
  if (i == 34) {
    start <- ((i-1)*100) + 1
    end <- start + 97
  }
  hdb_info_missing[start:end, 3:4] <- suppressMessages(geocode(hdb_info_missing$address[start:end], output = "latlon"))
  print(paste(start, "to", end, "completed"))
  print(Sys.time())
}

## Put the data together

hdb_coords_all <- hdb_info_missing[,c(1,3:4)] %>%
  bind_rows(addresses_geocoded[,c(1,3:4)]) %>%
  mutate(address = str_replace(address, "Block ", ""),
         address = str_replace(address, "COMMONWEALTH", "C'WEALTH")) 

resaledata_final <- resaledata %>%
  left_join(hdb_coords_all) %>% # including the lat/lon information in the original dataset
  mutate(address = str_replace(address, " Singapore", ""))

write_csv(resaledata_final, "resaledata.csv")

## Finding out the top/bottom 10 HDB flats for each category

top10_price <- resaledata_final %>%
  top_n(10, adj_resale_price) %>%
  mutate(grp = "top10_price")

bot10_price <- resaledata_final %>%
  top_n(-10, adj_resale_price) %>%
  mutate(grp = "bot10_price")

top10_duration <- resaledata_final %>%
  group_by(flat_type, flat_model, address, lease_commence_date, floor_area_sqm) %>%
  arrange(transaction_month) %>%
  mutate(duration = transaction_month - lag(transaction_month)) %>%
  ungroup() %>%
  top_n(10, duration)  %>%
  mutate(grp = "top10_duration")

top10_size <- resaledata_final %>%
  top_n(100, floor_area_sqm) %>%
  mutate(flat_model = tolower(flat_model)) %>%
  distinct(flat_type, block, street_name, lease_commence_date, floor_area_sqm, flat_model, .keep_all = TRUE) %>%
  arrange(desc(floor_area_sqm)) %>%
  .[1:10,] %>%
  mutate(grp = "top10_size")

top10_price_all <- resaledata_final %>%
  semi_join(top10_price, by = c('address', 'storey_range', 'floor_area_sqm', 'flat_model')) %>%
  group_by(address, storey_range, floor_area_sqm) %>%
  arrange(desc(transaction_month)) %>%
  mutate(grp = "top10_price_all")

## Binding all of the data together to consolidate it into one dataframe (for convenience)

final <- top10_price %>%
  bind_rows(bot10_price) %>%
  bind_rows(top10_duration) %>%
  bind_rows(top10_size) %>%
  bind_rows(top10_price_all) %>%
  mutate(flat_model = str_to_title(flat_model),
         address = str_to_title(address),
         storey_range = tolower(storey_range),
         transaction_month = format(transaction_month, "%b %Y"))

saveRDS(final, "final.rds")
