# empaticaR
R package to read and organize data from Empatica EmbracePlus watch

## Installation
Install using devtools
```
devtools::install_github("brian-lau/empaticaR")
```

## Usage
We'll start by pulling down some data
```
library(tidyverse)
library(magrittr)

library(s3fs)
library(sparklyr)

Sys.setenv("AWS_ACCESS_KEY_ID" = "INSERT_KEY",
           "AWS_SECRET_ACCESS_KEY" = "INSERT_SECRET_KEY",
           "AWS_DEFAULT_REGION" = "DEFAULT_REGION")

s3_basepath <- "STUDY_S3_PATH"

# Return list of all files
# allfiles <- s3_dir_ls(path = s3_basepath, recurse = T)

# Single file download
#path = paste0("s3://", s3_basepath, "REST_OF_PATH_TO_FILE")
#s3_file_download(path = path)

# Directory download
#path = paste0("s3://", s3_basepath, "REST_OF_PATH_TO_DIR")
savepath = "~/Downloads/2023-12-01/"
s3_dir_download(path = path, savepath)

# Read avro data
basepath = "PATH_TO_AVRO_DATA"

sc <- sparklyr::spark_connect(master = "local", version = "3.5.0", packages = c("avro"))

# Read individual avro file
df = read_empatica_avro_file(sc = sc, path = paste0(basepath, "1-1-01_1701129261.avro"))

# Read all avro files in directory
df_all = read_empatica_avro_dir(sc = sc, path = basepath)
```
