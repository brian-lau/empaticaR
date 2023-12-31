% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rawdata.R
\name{parse_empatica_rawdata}
\alias{parse_empatica_rawdata}
\title{parse_empatica_rawdata: Parse lists of rawdata from AVRO file}
\usage{
parse_empatica_rawdata(
  rawdata,
  timebase = "seconds",
  varnames = c("accelerometer", "gyroscope", "eda", "temperature", "tags", "bvp",
    "systolicPeaks", "steps")
)
}
\arguments{
\item{rawdata}{Rawdata list from AVRO file}

\item{timebase}{Reserved to allow requesting timebase (sec, msec, )}

\item{varnames}{Data fields to request}
}
\value{
A nested data frame
}
\description{
parse_empatica_rawdata: Parse lists of rawdata from AVRO file
}
\examples{
basepath = "/Users/brian/Downloads/2023-11-27/01-3YK3J151FF/raw_data/v6/"
sc <- sparklyr::spark_connect(master = "local", version = "3.5.0", packages = c("avro"))
path = paste0(basepath, "1-1-01_1701129261.avro")
df <- sparklyr::spark_read_avro(sc, path = path)
df_raw <- parse_empatica_rawdata(df \%>\% pull(rawData))
}
