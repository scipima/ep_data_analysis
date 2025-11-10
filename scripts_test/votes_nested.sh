#!/usr/bin/env bash 

# Get all the Pleanry Session Documents of the day
curl -X 'GET' \
 'https://data.europarl.europa.eu/api/v2/plenary-session-documents/PV-9-2023-11-21?format=application%2Fld%2Bjson&language=en' \
 -H 'accept: application/ld+json' \
 -H 'User-Agent: renew_parlwork-prd-2.0.0' | jq .

