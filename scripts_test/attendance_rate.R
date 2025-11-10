###--------------------------------------------------------------------------###
# Get the overall distribution of types of vote for Renew ----------------------
meps_rcv_mandate[, result := ifelse(test = result %in% c(-3, -2, 0L),
                                 yes = "Absent", no = "Voted")]
attendance_df <- meps_rcv_mandate[, list(
  .N), 
  keyby = list(country_id, polgroup_id, natparty_id, result)]
attendance_df[, sum_votes := sum(N), keyby = list(polgroup_id, natparty_id)] 
p = attendance_df[, attendance_rate := N / sum_votes
                  # ][, c("N", "sum_votes") := NULL
                    ] |> 
  join_polit_labs() |> 
  join_meps_countries() |> 
  filter(result == "Absent" 
         & !is.na(national_party)
         & !natparty_id %in% natparties_independent_ids) |> 
  group_by(country) |>
  dplyr::slice_max(attendance_rate, n = 1) |> 
  dplyr::arrange(country,attendance_rate) 
fwrite(p, here::here("data_out","attendance_rate.csv") )
