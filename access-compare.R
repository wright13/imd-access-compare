library(magrittr)

# Set db paths
db_one <- "./SFCN_Periphyton_20230802_Backup.accdb"
db_two <- "./SFCN_Periphyton_20230802_Backup_v2.accdb"

# Connect to databases
conn_one <- RODBC::odbcConnectAccess2007(db_one)
conn_two <- RODBC::odbcConnectAccess2007(db_two)

# Compare list of tables
tables_one <- RODBC::sqlTables(conn_one) %>%
  dplyr::select(TABLE_NAME, TABLE_TYPE, TABLE_SCHEM) %>%
  dplyr::filter(TABLE_TYPE %in% c("TABLE", "VIEW"))

tables_two <- RODBC::sqlTables(conn_two) %>%
  dplyr::select(TABLE_NAME, TABLE_TYPE, TABLE_SCHEM) %>%
  dplyr::filter(TABLE_TYPE %in% c("TABLE", "VIEW"))

diff_tables <- daff::diff_data(tables_one, tables_two)
daff::render_diff(diff_tables)

# Where table names match, compare rows
tables_both <- dplyr::inner_join(tables_one, tables_two, by = "TABLE_NAME") %>%
  dplyr::pull(TABLE_NAME)

diff_rows <- sapply(tables_both, function(table){
  data_one <- RODBC::sqlFetch(conn_one, table, stringsAsFactors = FALSE)
  data_two <- RODBC::sqlFetch(conn_two, table, stringsAsFactors = FALSE)
  if (!is.null(nrow(data_one)) &&  !is.null(nrow(data_two)) &&
      !isTRUE(all.equal(data_one, data_two, ignore_col_order = FALSE, ignore_row_order = FALSE, convert = FALSE))) {
    diff_data <- tryCatch(daff::diff_data(data_one, data_two),
                          error = glue::glue("Error reading or comparing {table}")
                          )
  } else {
    diff_data <- glue::glue("Error reading or comparing {table}")
  }
  if ("data_diff" %in% class(diff_data)) {
    daff::render_diff(diff_data, file = glue::glue("diffs/{table}.html"), view = FALSE, title = table)

  }
  return(diff_data)

}, simplify = TRUE, USE.NAMES = TRUE)

RODBC::odbcCloseAll()
