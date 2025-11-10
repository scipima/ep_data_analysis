###--------------------------------------------------------------------------###
# Parse RCV XML Files ----------------------------------------------------------
###--------------------------------------------------------------------------###

#' Helper function to extract MEP votes from political group nodes
extract_mep_votes <- function(group_nodes, vote_result) {
  if (length(group_nodes) == 0) return(data.frame())
  
  mep_votes <- list()
  
  for (group_node in group_nodes) {
    political_group <- xml2::xml_attr(group_node, "Identifier")
    
    # Find member names within this group - use the correct element name
    members <- xml2::xml_find_all(group_node, ".//PoliticalGroup.Member.Name")
    
    if (length(members) > 0) {
      member_data <- data.frame(
        political_group = political_group,
        member_name = xml2::xml_text(members),
        mep_id = xml2::xml_attr(members, "MepId"),
        pers_id = xml2::xml_attr(members, "PersId"),
        vote_result = vote_result,
        stringsAsFactors = FALSE
      )
      
      mep_votes[[length(mep_votes) + 1]] <- member_data
    }
  }
  
  if (length(mep_votes) > 0) {
    return(data.table::rbindlist(mep_votes, fill = TRUE))
  } else {
    return(data.frame())
  }
}

#' Helper function to extract intention votes (corrections)
extract_intention_votes <- function(intentions_node) {
  if (xml2::xml_length(intentions_node) == 0) return(data.frame())
  
  intention_votes <- list()
  
  # Process For intentions - use direct Member.Name elements
  for_intentions <- xml2::xml_find_all(intentions_node, ".//Intentions.Result.For//Member.Name")
  if (length(for_intentions) > 0) {
    for_data <- data.frame(
      political_group = NA_character_,  # Intentions don't have political group structure
      member_name = xml2::xml_text(for_intentions),
      mep_id = xml2::xml_attr(for_intentions, "MepId"),
      pers_id = xml2::xml_attr(for_intentions, "PersId"),
      vote_result = "Intention.For",
      stringsAsFactors = FALSE
    )
    if (nrow(for_data) > 0) {
      intention_votes[["for"]] <- for_data
    }
  }
  
  # Process Against intentions
  against_intentions <- xml2::xml_find_all(intentions_node, ".//Intentions.Result.Against//Member.Name")
  if (length(against_intentions) > 0) {
    against_data <- data.frame(
      political_group = NA_character_, 
      member_name = xml2::xml_text(against_intentions),
      mep_id = xml2::xml_attr(against_intentions, "MepId"),
      pers_id = xml2::xml_attr(against_intentions, "PersId"),
      vote_result = "Intention.Against",
      stringsAsFactors = FALSE
    )
    if (nrow(against_data) > 0) {
      intention_votes[["against"]] <- against_data
    }
  }
  
  # Process Abstention intentions
  abstention_intentions <- xml2::xml_find_all(intentions_node, ".//Intentions.Result.Abstention//Member.Name")
  if (length(abstention_intentions) > 0) {
    abstention_data <- data.frame(
      political_group = NA_character_, 
      member_name = xml2::xml_text(abstention_intentions),
      mep_id = xml2::xml_attr(abstention_intentions, "MepId"),
      pers_id = xml2::xml_attr(abstention_intentions, "PersId"),
      vote_result = "Intention.Abstention",
      stringsAsFactors = FALSE
    )
    if (nrow(abstention_data) > 0) {
      intention_votes[["abstention"]] <- abstention_data
    }
  }
  
  if (length(intention_votes) > 0) {
    return(data.table::rbindlist(intention_votes, fill = TRUE))
  } else {
    return(data.frame())
  }
}

#' Function to parse European Parliament RCV XML files into structured data
#' 
#' @param xml_path Path to the XML file
#' @return A list containing metadata and individual votes
#' 
parse_rcv_xml <- function(xml_path) {
  
  library(xml2)
  library(data.table)
  library(dplyr)
  
  # Read XML file
  xml_doc <- xml2::read_xml(xml_path)
  
  # Extract session metadata from root attributes
  session_metadata <- data.frame(
    sitting_identifier = xml2::xml_attr(xml_doc, "Sitting.Identifier"),
    sitting_date = xml2::xml_attr(xml_doc, "Sitting.Date"),
    ep_reference = xml2::xml_attr(xml_doc, "EP.Reference"),
    ep_number = xml2::xml_attr(xml_doc, "EP.Number"),
    document_language = xml2::xml_attr(xml_doc, "Document.Language"),
    stringsAsFactors = FALSE
  )
  
  # Remove secret votes (as in your original code)
  rollcall_results <- xml2::xml_find_all(xml_doc, ".//RollCallVote.Result")
  secret_votes <- xml2::xml_find_first(rollcall_results, xpath = "Result.Secret") |>
    xml2::xml_length()
  
  if (any(secret_votes > 0)) {
    nodes_to_drop <- which(secret_votes > 0)
    xml2::xml_remove(rollcall_results[nodes_to_drop])
    # Refresh the list after removal
    rollcall_results <- xml2::xml_find_all(xml_doc, ".//RollCallVote.Result")
  }
  
  # Initialize result list
  all_votes <- list()
  
  # Process each vote
  for (i in seq_along(rollcall_results)) {
    vote_node <- rollcall_results[i]
    
    # Extract vote metadata
    vote_metadata <- data.frame(
      vote_number = i,
      rcv_id = xml2::xml_attr(vote_node, "Identifier"),
      dlv_id = xml2::xml_attr(vote_node, "DlvId"), 
      vote_date = xml2::xml_attr(vote_node, "Date"),
      description = xml2::xml_text(xml2::xml_find_first(vote_node, ".//RollCallVote.Description.Text")),
      stringsAsFactors = FALSE
    )
    
    # Extract vote tallies
    result_for <- xml2::xml_find_first(vote_node, "./Result.For")
    result_against <- xml2::xml_find_first(vote_node, "./Result.Against") 
    result_abstention <- xml2::xml_find_first(vote_node, "./Result.Abstention")
    
    vote_metadata$result_for = as.numeric(xml2::xml_attr(result_for, "Number"))
    vote_metadata$result_against = as.numeric(xml2::xml_attr(result_against, "Number"))
    vote_metadata$result_abstention = as.numeric(xml2::xml_attr(result_abstention, "Number"))
    
    # Extract individual MEP votes
    individual_votes <- list()
    
    # Process For votes
    if (!is.na(vote_metadata$result_for) && vote_metadata$result_for > 0) {
      for_groups <- xml2::xml_find_all(result_for, ".//Result.PoliticalGroup.List")
      for_votes <- extract_mep_votes(for_groups, "For")
      if (nrow(for_votes) > 0) {
        individual_votes$for_votes <- for_votes
      }
    }
    
    # Process Against votes  
    if (!is.na(vote_metadata$result_against) && vote_metadata$result_against > 0) {
      against_groups <- xml2::xml_find_all(result_against, ".//Result.PoliticalGroup.List")
      against_votes <- extract_mep_votes(against_groups, "Against")
      if (nrow(against_votes) > 0) {
        individual_votes$against_votes <- against_votes
      }
    }
    
    # Process Abstention votes
    if (!is.na(vote_metadata$result_abstention) && vote_metadata$result_abstention > 0) {
      abstention_groups <- xml2::xml_find_all(result_abstention, ".//Result.PoliticalGroup.List")
      abstention_votes <- extract_mep_votes(abstention_groups, "Abstention")
      if (nrow(abstention_votes) > 0) {
        individual_votes$abstention_votes <- abstention_votes
      }
    }
    
    # Process Intentions (corrections/intended votes)
    intentions_node <- xml2::xml_find_first(vote_node, "./Intentions")
    if (xml2::xml_length(intentions_node) > 0) {
      intention_votes <- extract_intention_votes(intentions_node)
      if (nrow(intention_votes) > 0) {
        individual_votes$intention_votes <- intention_votes
      }
    }
    
    # Combine all individual votes for this vote
    all_individual_votes <- data.table::rbindlist(individual_votes, fill = TRUE, idcol = "vote_type")
    
    # Add vote metadata to each row
    if (nrow(all_individual_votes) > 0) {
      all_individual_votes <- cbind(vote_metadata[rep(1, nrow(all_individual_votes)), ], 
                                   all_individual_votes)
    }
    
    all_votes[[i]] <- list(
      metadata = vote_metadata,
      individual_votes = all_individual_votes
    )
  }
  
  # Combine all votes into final datasets
  final_result <- list(
    session_metadata = session_metadata,
    vote_metadata = data.table::rbindlist(lapply(all_votes, function(x) x$metadata), fill = TRUE),
    individual_votes = data.table::rbindlist(lapply(all_votes, function(x) x$individual_votes), fill = TRUE)
  )
  
  return(final_result)
}

# Test the function with your XML file
if (FALSE) {
  # Example usage:
  xml_path <- here::here("data_in", "test_in", "test_xml", "PV-10-2025-10-21-RCV_FR.xml")
  
  # Parse the XML
  rcv_data <- parse_rcv_xml(xml_path)
  
  # View the results
  print("Session metadata:")
  print(rcv_data$session_metadata)
  
  print("\nVote metadata:")
  print(head(rcv_data$vote_metadata))
  
  print("\nIndividual votes:")
  print(head(rcv_data$individual_votes))
  
  # Save to files
  data.table::fwrite(rcv_data$vote_metadata, "vote_metadata.csv")
  data.table::fwrite(rcv_data$individual_votes, "individual_votes.csv")
}