#' read_empatica_avro_dir: read multiple AVRO files in directory
#'
#' @param path Path of directory
#' @param sc A Spark connection
#'
#' @return A nested data frame with a row for each AVRO file
#' @export
#'
#' @examples
#' basepath = "/Users/brian/Downloads/2023-11-27/01-3YK3J151FF/raw_data/v6/"
#' sc <- sparklyr::spark_connect(master = "local", version = "3.5.0", packages = c("avro"))
#' df = read_empatica_avro_dir(sc = sc, path = basepath)
read_empatica_avro_dir <- function(path,
                                   sc = NULL
) {
  # Structure
  # [study_id]-[site_id]-[participant_id]_[timestamp].avro
  # The Avro filename contains the start timestamp,
  # in Unix timestamp (s), of the file content.
  filenames <- dir(path)
  # Sort filenames just in case
  # TODO
  DF = list()
  for (f in filenames) {
    DF[[f]] <- read_empatica_avro_file(sc = sc, path = paste0(basepath, f))
  }

  df <- dplyr::bind_rows(DF, .id = "filename")

  # Split out start time of the AVRO file
  #https://stackoverflow.com/questions/39086400/extracting-a-string-between-other-two-strings-in-r
  df %<>%
    rowwise() %>%
    mutate(t_start_file = as.numeric(str_match(filename, "_\\s*(.*?)\\s*\\.")[[2]]),
           .after = filename) %>%
    ungroup() %>%
    mutate(t_start_file = anytime::anytime(t_start_file, tz = "UTC")) # CHECK

  # Sanity check
  # EmbracePlus creates a new Avro file about every 15 minutes of data collected.
  # A single Avro file contains a continuous portion of data, meaning that in
  # case of any gap (e.g., because of a charging period) a new file is created.

  # Use accelerometer, appears always present and has highest Fs
  df %<>%
    rowwise() %>%
    mutate(t_start = accelerometer$t[1],
           t_end = accelerometer$t %>% tail(n=1),
           .after = t_start_file) %>%
    ungroup()

  # Time between last sample of previous file and first sample of current file
  df %<>%
    mutate(t_gap = t_start - dplyr::lag(t_end, n = 1), .after = t_end) %>%
    mutate(t_gap2 = t_start - t_start_file, .after = t_gap)
}

#' read_empatica_avro_file: Read single AVRO file
#'
#' @param path Path to file
#' @param sc A Spark connection
#' @param spark_version
#' @param spark_disconnect_on_exit
#' @param collect_on_exit
#' @param parse_rawdata
#' @param ...
#'
#' @return A nested data frame
#' @export
#'
#' @examples
#' basepath = "/Users/brian/Downloads/2023-11-27/01-3YK3J151FF/raw_data/v6/"
#' sc <- spark_connect(master = "local", version = "3.5.0", packages = c("avro"))
#' df = read_empatica_avro_file(sc = sc, path = paste0(basepath, "1-1-01_1701129261.avro"))
read_empatica_avro_file <- function(path,
                                    sc = NULL,
                                    spark_version = "3.5.0",
                                    spark_disconnect_on_exit = FALSE,
                                    collect_on_exit = TRUE,
                                    parse_rawdata = TRUE,
                                    ...
){
  if (is.null(sc)) {
    message("Initiating Spark connection.\n")
    sc <- spark_connect(master = "local",
                        version = spark_version,
                        packages = "avro")
  }

  df <- sparklyr::spark_read_avro(sc, path = path) %>%
    collect()

  if (spark_disconnect_on_exit) {
    df %<>% collect()
    message("Closing Spark connection.\n")
    sparklyr::spark_disconnect(sc)
    collect_on_exit <- TRUE
  }

  if (collect_on_exit & !spark_disconnect_on_exit)
    df %<>% collect()

  if (parse_rawdata & collect_on_exit) {
    df_raw <- parse_empatica_rawdata(df %>% pull(rawData), ...)
    df %<>% select(-rawData) %>% bind_cols(df_raw)
  }

  # Flatten versioning information & enrollment
  df %<>% mutate(across(c(schemaVersion, fwVersion, hwVersion, algoVersion),
                        ~ unlist(.x) %>% paste0(collapse = ".")))
  df_enroll <- df$enrollment %>% unlist() %>% rev() %>% tibble::as_tibble_row()
  df <- bind_cols(df_enroll, df %>% select(-enrollment))

  return(df)
}

#' parse_empatica_rawdata: Parse lists of rawdata from AVRO file
#'
#' @param rawdata Rawdata list from AVRO file
#' @param timebase Reserved to allow requesting timebase (sec, msec, )
#' @param varnames Data fields to request
#'
#' @return A nested data frame
#' @export
#'
#' @examples
#' basepath = "/Users/brian/Downloads/2023-11-27/01-3YK3J151FF/raw_data/v6/"
#' sc <- sparklyr::spark_connect(master = "local", version = "3.5.0", packages = c("avro"))
#' path = paste0(basepath, "1-1-01_1701129261.avro")
#' df <- sparklyr::spark_read_avro(sc, path = path)
#' df_raw <- parse_empatica_rawdata(df %>% pull(rawData))
parse_empatica_rawdata <- function(rawdata,
                                   timebase = "seconds",
                                   varnames = c("accelerometer",
                                                "gyroscope",
                                                "eda",
                                                "temperature",
                                                "tags",
                                                "bvp",
                                                "systolicPeaks",
                                                "steps"
                                   )
) {
  # accelerometer: Fs = 64
  # gyroscope: Fs = NA?
  # eda: Fs = 4, μS
  # temperature: Fs = 64, °C
  # steps: Fs = 0.2
  # https://manuals.empatica.com/ehmp/careportal/data_access/v2.3e/en.pdf
  # See pg. 13
  # https://s3.amazonaws.com/box.empatica.com/manuals/ehmp/careportal/data_access/v1.4e/en/Data%20Access%20Documentation_PRSPEC-2%20REV10.0.pdf
  # https://s3.amazonaws.com/box.empatica.com/manuals/ehmp/careportal/data_access/v1.4e/en/Data%20Access%20Documentation_PRSPEC-2%20REV11.0.pdf

  if ((length(rawdata) == 1) & (lengths(rawdata) > 1)) {
    rawdata <- rawdata[[1]]
  } else {
    # error as unrecognized
    # TODO
  }

  VARS = list()
  for (var in varnames) {
    if (var == "tags") {
      t_utc <- anytime::utctime(rawdata[[var]]$peaksTimeMicros / 1e6)
      VARS[[var]] <- tibble(t = t_utc)
    } else if (var == "systolicPeaks") {
      t_utc <- anytime::utctime(rawdata[[var]]$peaksTimeNanos / 1e9)
      VARS[[var]] <- tibble(t = t_utc)
    } else {
      n <- ifelse(!is.null(rawdata[[var]]$values),
                  length(rawdata[[var]]$values),
                  length(rawdata[[var]]$x))
      t_start <- rawdata[[var]]$timestampStart / 1e6
      t_utc <- (1:n) - 1
      Fs <- rawdata[[var]]$samplingFrequency
      t_utc = anytime::utctime(t_utc/Fs + t_start)

      if (n > 0) {
        if (var %in% c("accelerometer", "gyroscope")) {
          VARS[[var]] <- tibble(
            t = t_utc,
            x = rawdata[[var]]$x,
            y = rawdata[[var]]$y,
            z = rawdata[[var]]$z,
          )

          # Convert ADC counts in g
          # From Data Access Documention v.10, pg. 35
          delta_physical = rawdata[[var]]$imuParams$physicalMax -
            rawdata[[var]]$imuParams$physicalMin
          delta_digital = rawdata[[var]]$imuParams$digitalMax -
            rawdata[[var]]$imuParams$digitalMin
          VARS[[var]] %<>% mutate(across(c(x,y,z), ~.x*delta_physical/delta_digital))
        } else {
          VARS[[var]] <- tibble(
            t = t_utc,
            values = rawdata[[var]]$values
          )
        }
      } else {
        message(paste0(var, " requested, but no data found. Skipping ..."))
      }
    }
  }

  df <- tibble::enframe(VARS) %>% tidyr::pivot_wider()
}
