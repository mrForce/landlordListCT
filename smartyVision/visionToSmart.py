import csv
import argparse
import collections
import itertools
from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup



parser = argparse.ArgumentParser(description='Take output from Vision GIS, run building address and owner address through SmartyStreets')
parser.add_argument('inputTSV', help='Output from Vision script')
parser.add_argument('cities', help='File containing list of cities within the municipality, seperated by newlines. First should be the first city to try. Note that these are USPS cities. Example: Cos Cob in the town of Greenwich')
parser.add_argument('landuse', help='File containing land use codes we care about')
parser.add_argument('output', help='Output TSV')

args = parser.parse_args()



def extractLines(fileName):
    lines = []
    with open(fileName, 'r') as f:
        for x in f:
            xStrip = x.strip()
            if len(xStrip) > 0:
                lines.append(xStrip.upper())
    return lines
state = 'CT'
cities = extractLines(args.cities)
landUseCodes = set(extractLines(args.landuse))
visionHeaders = None
streets = collections.defaultdict(list)
with open(args.inputTSV, 'r') as f:
    reader = csv.DictReader(f, delimiter='\t')
    visionHeaders = reader.fieldnames
    assert('landuse' in visionHeaders)
    for row in reader:
        if row['landuse'].strip().upper() in landUseCodes:
            streets[row['street'].strip()].append(dict(row))

streetToCityIndex = {}
for streetName in streets.keys():
    streets[streetName].sort(key=lambda x: int(x['location'].split()[0]) if x['location'].split()[0].isdigit() else 0)
    streetToCityIndex[streetName] = 0

batchSize = min(100, len(streets))

recordIter = itertools.zip_longest(*streets.values())

creds = StaticCredentials('87f20fb5-a479-c2a6-61fc-f97766549421', 'aPjLRYlSqap2Q30bO5eM')

client = ClientBuilder(creds).build_us_street_api_client()

class Query:
    def __init__(self, streetAddress, secondary, city, state, street):
        self.streetAddress = streetAddress
        self.secondary = secondary
        self.city = city
        self.state = state
        self.street = street
    def setCity(self, city):
        self.city = city
    def makeStreetLookup(self, input_id):
        x = StreetLookup()
        x.input_id = input_id
        x.street = self.streetAddress
        x.secondary = self.secondary
        x.city = city
        x.state = state
        x.match = 'strict'
        return x

def breakLocation(location, street):
    #Extract out any secondary parts of the location, like unit numbers. Return a tuple (primary, secondary)
    assert(street in location)
    streetStartIndex = location.find(street)
    return (location[0:(streetStartIndex + len(street))].strip(), location[(streetStartIndex + len(street))::].strip())


outputFile = open(args.output, 'w')

fieldnames = visionHeaders + ['location_valid', 'location_components', 'location_analysis', 'location_metadata', 'location_footnote_Csharp', 'location_footnote_Dsharp', 'location_footnote_Fsharp', 'location_footnote_Hsharp', 'location_footnote_Isharp', 'location_footnote_Ssharp', 'location_footnote_Vsharp', 'location_footnote_Wsharp']

fieldnames.extend(['owner_address_valid', 'owner_address_components', 'owner_address_analysis', 'owner_address_metadata', 'owner_address_footnote_Csharp', 'owner_address_footnote_Dsharp', 'owner_address_footnote_Fsharp', 'owner_address_footnote_Hsharp', 'owner_address_footnote_Isharp', 'owner_address_footnote_Ssharp', 'owner_address_footnote_Vsharp', 'owner_address_footnote_Wsharp'])
while True:
    streetHeads = next(recordIter)
    if not any(streetHeads):
        break
    queries = []
    while len(streetHeads) > 0:
        while len(queries) < batchSize and len(streetHeads) > 0:
            head  = streetHeads.pop()
            if head:
                city = cities[streetToCityIndex[head['street']]]
                primary, secondary = breakLocation(head['location'], head['street'])
                queries.append(Query(primary, secondary, city, state))

        if queries:
            lookups = [queries[i].makeStreetLookup(i) for i in range(0, len(queries))]
            batch = Batch()
            for x in lookups:
                batch.add(x)
            assert(len(batch) == len(lookups))
            try:
                client.send_batch(batch)
            except exceptions.SmartyException as err:
                print(err)
                return
            for i, lookup in enumerate(batch):
                candidates = lookup.result                
                if len(candidates) == 0:
                    street = queries[i].street
                    cityIndex = streetToCityIndex[street]
                    if cityIndex == len(cities) - 1:
                        streetToCityIndex[street] = 0
                        #TODO Invalid address. Place into spreadsheet
                        queries[i] = None
                    else:
                        streetToCityIndex[street] += 1
                        queries[i].city = cities[streetToCityIndex[street]]
                else:
                    components = candidates[0].components
                    metadata = candidates[0].metadata
                    analysis = candidates[0].analysis
                    queries[i] = None
                    #TODO Valid address. Place necessary information into spreadsheet. 
            queries = [i for i in queries if i]                      
        
        
