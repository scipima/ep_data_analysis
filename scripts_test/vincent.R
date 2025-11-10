source(file = here::here("scripts_r", "aggregate_rcv.R"))

rcv_tmp = votes_dt$rcv_id[
    grepl(pattern = "Verts/ALE", x = votes_dt$responsible_organization_label_en)
    & votes_dt$inverse_consists_of == "eli/dl/event/MTG-PL-2025-03-12-VOT-ITM-965280"
]

p = tally_bygroup_byrcv[
    rcv_id %in% rcv_tmp
    & polgroup_id == 7035
] |>
    join_polit_labs() |>
    select(-result) |>
    pivot_wider(names_from = result_fct, values_from = tally) |>
    left_join(y = votes_dt[, list(rcv_id, headingLabel_en, referenceText_en,
                                  activity_label_en, comment_en)],
              by = "rcv_id")
