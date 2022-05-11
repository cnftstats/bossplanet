# Setup --------------------------------------------------------------------------------------------
library(data.table)
library(lubridate)
library(jsonlite)
library(tidyr)
library(rvest)
library(httr)


# Variables ----------------------------------------------------------------------------------------
link <- "https://cardano-mainnet.blockfrost.io/api/v0/assets/"
token <- sample(c("mainnetghJieJ39JMUlumgj2HwbxtNmEcY3WzC9",
                  "mainnetYBrF03aUZhsJaexjOO6z7pI6vuNxW0sv",
                  "mainnetvPXz1w2LMEpWQ8VdU5U9TBRsCuuwmh0g"), 1)
policy_id <- "5a2cdc6e3aa9612fe4676672f443e7efd39c309d45e7919a4bf27750"
project <- "Boss Planet Real Estate"
time_now <- as_datetime(now())

RAR <- readRDS("data/RAR.rds")


# Functions ----------------------------------------------------------------------------------------
extract_num <- function(x) as.numeric(gsub("[^0-9\\-]+","",as.character(x)))

loj <- function (X = NULL, Y = NULL, onCol = NULL) {
  if (truelength(X) == 0 | truelength(Y) == 0) 
    stop("setDT(X) and setDT(Y) first")
  n <- names(Y)
  X[Y, `:=`((n), mget(paste0("i.", n))), on = onCol]
}


# Extract information from jpg.store ---------------------------------------------------------------
# jpg.store/api/policy - all supported policies
# jpg.store/api/policy/[id]/listings - listings for a given policy
# jpg.store/api/policy/[id]/sales - sales for a given policy
JPG_list <- list()
p <- 1
while (TRUE) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/listings?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  if (nrow(X) == 0) break
  JPG_list[[p]] <- X
  X[, page := p]
  p <- p + 1
}

JPG <- rbindlist(JPG_list)

JPG[, link           := paste0("https://www.jpg.store/asset/", asset_id)]
JPG[, asset          := display_name]
JPG[, price          := price_lovelace/10**6]
JPG[, sc             := "yes"]
JPG[, market         := "jpg.store"]
JPG[, xcoord         := extract_num(strsplit(asset, ",")[[1]][1]), 1:nrow(JPG)]
JPG[, ycoord         := extract_num(strsplit(asset, ",")[[1]][2]), 1:nrow(JPG)]

loj(JPG, RAR[, .(asset_id, district, xcoord, ycoord)], "asset_id")

JPG <- JPG[, .(asset, type = "listing", price, district, last_offer = NA, sc, market, link,
               xcoord, ycoord)]


# JPG sales ----------------------------------------------------------------------------------------
JPGS_list <- lapply(1:5, function(p) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/sales?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  return(X)
})

JPGS <- rbindlist(JPGS_list)

JPGS[, asset          := display_name]
JPGS[, price          := price_lovelace/10**6]
JPGS[, market         := "jpg.store"]
JPGS[, sold_at        := as_datetime(confirmed_at)]
JPGS[, sold_at_hours  := difftime(time_now, sold_at, units = "hours")]
JPGS[, sold_at_days   := difftime(time_now, sold_at, units = "days")]
JPGS[, xcoord         := extract_num(strsplit(asset, ",")[[1]][1]), 1:nrow(JPGS)]
JPGS[, ycoord         := extract_num(strsplit(asset, ",")[[1]][2]), 1:nrow(JPGS)]

loj(JPGS, RAR[, .(asset_id, district, xcoord, ycoord)], "asset_id")

JPGS <- JPGS[order(-sold_at), .(asset, type = "listing", price, district,
                                sold_at, sold_at_hours, sold_at_days, market, xcoord, ycoord)]
JPGS <- JPGS[sold_at_hours <= 24]


# Merge markets data -------------------------------------------------------------------------------
# Listings
DT <- copy(JPG)

# Sales
DTS <- copy(JPGS)

# Add data collection timestamp
DT[, data_date := time_now]
DTS[, data_date := time_now]

# Fix bug with listing call returning many times the same asset (JPG)
DT <- DT[!duplicated(asset)]
DTS <- DTS[!duplicated(asset)]


# Add districts ------------------------------------------------------------------------------------
DT[, district_txt := factor(district, labels = paste("District", 1:max(district)),
                            levels = 1:max(district))]
DT[, price_rank   := as.numeric(as.integer(factor(price)))]
DT[, price_rank   := (price_rank-min(price_rank))/(max(price_rank)-min(price_rank))]

DTS[, district_txt := factor(district, labels = paste("District", 1:max(district)),
                             levels = 1:max(district))]
DTS[, price_rank   := as.numeric(as.integer(factor(price)))]
DTS[, price_rank   := (price_rank-min(price_rank))/(max(price_rank)-min(price_rank))]


# Save ---------------------------------------------------------------------------------------------
saveRDS(DT, file = "data/DT.rds")
saveRDS(DTS, file = "data/DTS.rds")


# Database evolution -------------------------------------------------------------------------------
DTE <- copy(DT)
.file_name <- "data/DTE.rds"
if (file.exists(.file_name)) {
  cat("File data/DTE exists:", file.exists(.file_name), "\n")
  DTE_old <- readRDS(.file_name)
  DTE <- rbindlist(list(DTE, DTE_old), use.names = TRUE)
  DTE <- DTE[difftime(time_now, data_date, units = "hours") <= 24] # Only retain last 24 hours
}
saveRDS(DTE, file = .file_name)
