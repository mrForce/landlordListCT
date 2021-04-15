
from smartystreets_python_sdk import SharedCredentials, ClientBuilder
from smartystreets_python_sdk.us_autocomplete_pro import Lookup as AutocompleteProLookup, geolocation_type

creds = SharedCredentials('87f20fb5-a479-c2a6-61fc-f97766549421', 'aPjLRYlSqap2Q30bO5eM')
client = ClientBuilder(creds).build_us_autocomplete_pro_api_client()
lookup=AutocompleteProLookup('850 East Main Street')
lookup.add_city_filter('Stamford')
lookup.add_state_filter('CT')
suggestions = client.send(lookup)
for suggestion in suggestions:
    print(suggestion.street_line + ' '+  suggestion.secondary + ' ' + suggestion.city)


