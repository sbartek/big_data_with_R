source('configuration.R')

zip_files <- list.files(path = INSTACART_DATA_DIR, pattern = "*.zip")
for (ifile in zip_files) {
  unzip(file.path(INSTACART_DATA_DIR, ifile), exdir = DATA_DIR)
}

## https://www.kaggle.com/hugomathien/soccer
ffile <- list.files(path = FOOTBALL_DATA_DIR, pattern = "*.zip")[1]
unzip(file.path(FOOTBALL_DATA_DIR, ffile), exdir = DATA_DIR)
