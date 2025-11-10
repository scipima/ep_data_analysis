#!/usr/bin/env bash 

###--------------------------------------------------------------------------###
# Credentials
username='EP-ITER4PG_ALD-DV'
password='94sQi0R8'


###--------------------------------------------------------------------------###
# API Call
# curl --verbose --user "$username":"$password" 'http://iterdv.secure.ep.parl.union.eu/iter/api/rest/procedures/list-by-creation-date?beginDate=01/03/2024&endDate=30/03/2024'
# curl --verbose --user "$username":"$password" 'https://iterdv.secure.ep.parl.union.eu/iter/api'
curl --verbose --user 'EP-ITER4PG_ALD-DV':'94sQi0R8' 'http://iterdv.secure.ep.parl.union.eu/iter/api'

