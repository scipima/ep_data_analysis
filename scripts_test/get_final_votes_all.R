#------------------------------------------------------------------------------#
# Get final votes from Plenary Session Docs ------------------------------------
#------------------------------------------------------------------------------#

#' The API does not have complete data on Plenary Session Documents (as of 2025/05/14).
#' Thus, we extract the final votes from the text of the RCV results.

#------------------------------------------------------------------------------#
### DOC_ID; FINALS -------------------------------------------------------------
# quick and dirty way to get doc_id out of labels -----------------------------#
if ("activity_label_fr" %in% names(votes_dt) ) {
    votes_dt[
        grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution",
              x = activity_label_fr, ignore.case = TRUE, perl = TRUE),
        `:=`( is_final = 1L ) ]

    votes_dt[
        grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution",
              x = referenceText_fr, ignore.case = TRUE, perl = TRUE),
        `:=`( is_final = 1L ) ]

    # Deal with special cases
    votes_dt[
        # If this is present ...
        grepl(pattern = "r.solution|d.cision|approbation",
              x = activity_label_fr, ignore.case = TRUE, perl = TRUE)
        # ... AND this is NOT present ...
        & !grepl(pattern = "Am \\d+|§ \\d+|Consid.rant",
                 x = activity_label_fr, ignore.case = FALSE, perl = TRUE),
        `:=`( is_final =  1L ) ]

    # If NA, then 0
    votes_dt[is.na(is_final), is_final := 0L]

} else if ("activity_label_mul" %in% names(votes_dt) ) {

    # Flag for final ----------------------------------------------------------#
    votes_dt[, `:=`(
        is_final = data.table::fifelse(
            test = grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution",
                         x = activity_label_mul, ignore.case = TRUE, perl = TRUE),
            yes = 1L, no = 0L) ) ]

    # Deal with special cases
    votes_dt[, `:=`(
        is_final = data.table::fifelse(
            test = (
                # If this is present ...
                grepl(pattern = "r.solution|d.cision|approbation",
                      x = activity_label_mul, ignore.case = TRUE, perl = TRUE)
                # ... AND this is NOT present ...
                & !grepl(pattern = "Am \\d+|§ \\d+|Consid.rant",
                         x = activity_label_mul, ignore.case = FALSE, perl = TRUE)
            ),
            yes = 1L, no = is_final))]
}
