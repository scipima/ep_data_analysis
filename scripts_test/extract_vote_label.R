s = c(
    "Mobilisation du Fonds européen d’ajustement à la mondialisation: demande EGF/2024/002 BE/Limburg machinery and paper - Belgique - Mobilisation of the European Globalisation Adjustment Fund: application EGF/2024/002 BE/Limburg machinery and paper - Belgium - Inanspruchnahme des Europäischen Fonds für die Anpassung an die Globalisierung: Antrag EGF/2024/002 BE/Limburg Maschinenbau und Papier – Belgien",
    "Mesures spécifiques au titre du Fonds européen agricole pour le développement rural (Feader) pour les États membres touchés par des catastrophes naturelles - Specific measures under the European Agricultural Fund for Rural Development (EAFRD) for Member States affected by natural disasters - Spezifische Maßnahmen im Rahmen des Europäischen Landwirtschaftsfonds für die Entwicklung des ländlichen Raums (ELER) für von Naturkatastrophen betroffene Mitgliedstaaten",
    "Activités du Médiateur européen – rapport annuel 2023 - Activities of the European Ombudsman – annual report 2023 - Tätigkeit der Europäischen Bürgerbeauftragten – Jahresbericht 2023",
    "Matériels forestiers de reproduction de la catégorie «matériels testés», leur étiquetage et les noms des autorités responsables de l’admission et du contrôle de la production - Forest reproductive material of the ‘tested’ category - Forstliches Vermehrungsgut der Kategorie „geprüft“, dessen Etikettierung und die Namen der für Zulassung und Kontrolle der Erzeugung zuständigen Behörden",
    "Soutien régional d’urgence: RESTORE - Regional Emergency Support: RESTORE - Regionale Soforthilfe: RESTORE",
    "Règlement sur la déforestation: dispositions relatives à la date d’application - Deforestation Regulation: provisions relating to the date of application - Entwaldungsverordnung: Bestimmungen zum Geltungsbeginn",
    "Situation au Venezuela à la suite de l’usurpation de la présidence le 10 janvier 2025 - Situation in Venezuela following the usurpation of the presidency on 10 January 2025 - Lage in Venezuela nach der widerrechtlichen Machtergreifung am 10. Januar 2025",
    "Le cas de Boualem Sansal en Algérie - Case of Boualem Sansal in Algeria - Der Fall von Boualem Sansal in Algerien",
    "La répression systématique des droits de l’homme en Iran, notamment les cas de Pakhshan Azizi et Wrisha Moradi, et la prise en otage de citoyens de l’Union - Systematic repression of human rights in Iran, notably the cases of Pakhshan Azizi and Wrisha Moradi, and the taking of EU citizens as hostages - Systematische Unterdrückung der Menschenrechte in Iran, insbesondere die Fälle von Pexşan Ezîzî und Werîşe Mûradî, und die Geiselnahme von Unionsbürgern",
    "Désinformation et falsification de l’histoire par la Russie pour justifier sa guerre d’agression contre l’Ukraine - Russia’s disinformation and historical falsification to justify its war of aggression against Ukraine - Desinformation und Geschichtsfälschung seitens Russlands zur Rechtfertigung des Angriffskrieges gegen die Ukraine",
    "Accord de partenariat intérimaire CE-États du Pacifique: adhésion des Tonga - EC-Pacific States Interim Partnership Agreement: accession of Tonga - Interims-Partnerschaftsabkommen zwischen der EG und den Pazifik-Staaten: Beitritt Tongas",
    "Conclusion, au nom de l’Union européenne, de la convention des Nations unies sur la transparence dans l’arbitrage entre investisseurs et États fondé sur des traités - Conclusion, on behalf of the European Union, of the United Nations Convention on transparency in treaty-based investor-State arbitration - Abschluss – im Namen der Europäischen Union – des Übereinkommens der Vereinten Nationen über Transparenz in abkommensverankerten Investor-Staat-Schiedsverfahren",
    "Accord de partenariat intérimaire CE-États du Pacifique: adhésion des Tuvalu - EC-Pacific States Interim Partnership Agreement: accession of Tuvalu - Interims-Partnerschaftsabkommen EG/Pazifik-Staaten: Beitritt Tuvalus",
    "Accord de partenariat intérimaire CE-États du Pacifique: adhésion de Niue - EC-Pacific States Interim Partnership Agreement: accession of Niue - Interims-Partnerschaftsabkommen zwischen der EG und den Pazifik-Staaten: Beitritt Niues",
    "La constitution, les compétences, la composition numérique et la durée de mandat d’une commission spéciale sur le bouclier européen de la démocratie - Setting up a special committee on the European Democracy Shield, and defining its responsibilities, numerical strength and term of office - Einsetzung eines Sonderausschusses zu dem europäischen Schutzschild für die Demokratie und Festlegung seiner Zuständigkeiten, seiner zahlenmäßigen Zusammensetzung und seiner Mandatszeit",
    "La constitution, les compétences, la composition numérique et la durée de mandat d’une commission spéciale sur la crise du logement dans l’Union européenne - Setting up a special committee on the Housing Crisis in the European Union, and defining its responsibilities, numerical strength and term of office - Einsetzung eines Sonderausschusses zur Wohnraumkrise in der Europäischen Union und Festlegung seiner Zuständigkeiten, seiner zahlenmäßigen Zusammensetzung und seiner Mandatszeit",
    "Amendement à l’annexe VI – Attribution des commissions parlementaires permanentes - Amendment of Annex VI – Powers and responsibilities of the standing committees - Änderung von Anlage VI – Zuständigkeiten der ständigen Ausschüsse des Parlaments"
)

# List of split vectors
vote_labels_list = stringr::str_split(string = s, pattern = "\\s-\\s")

# Delimiter counts
delim_count = stringi::stri_count(str = s, regex = "\\s-\\s")
delim_first = delim_count/2
# idx = 2L

# Empty repository
vote_labels_en <- vector(mode = "list", length = length(s))

for (i_row in seq_along(s)) {

    # Extract string vector
    full_label = vote_labels_list[[ i_row ]]

    # Store ENG title
    vote_labels_en[[i_row]] <- paste0(
        full_label[ (cc_first[ i_row ] + 1) : cc[ i_row ] ],
        collapse = " - ")
}

