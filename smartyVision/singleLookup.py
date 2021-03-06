
from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup

creds = StaticCredentials('87f20fb5-a479-c2a6-61fc-f97766549421', 'aPjLRYlSqap2Q30bO5eM')


client = ClientBuilder(creds).build_us_street_api_client()

lookup = StreetLookup()
lookup.input_id='1'
lookup.street='25 BUTLER STREET'
lookup.city='GREENWICH'
lookup.state='CT'
lookup.match='strict'

client.send_lookup(lookup)

result = lookup.result

for candidate in result:
    print('new candidate')
    print(vars(candidate.components))
    print(vars(candidate.metadata))
    print(vars(candidate.analysis))
    print(vars(candidate))
    print(candidate.analysis.footnotes)
