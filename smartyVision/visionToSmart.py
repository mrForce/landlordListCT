import csv
import argparse
import collections
import itertools
import json
from smartystreets_python_sdk import StaticCredentials, exceptions, Batch, ClientBuilder
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
numCities = len(cities)
assert(numCities >= 1)
landUseCodes = set(extractLines(args.landuse))
visionHeaders = None

outputFile = open(args.output, 'w')

fieldnames = visionHeaders + ['location_valid', 'city', 'zip', 'plus4_code', 'delivery_point', 'location_components', 'location_footnotes', 'location_metadata', 'location_footnote_Csharp', 'location_footnote_Dsharp', 'location_footnote_Fsharp', 'location_footnote_Hsharp', 'location_footnote_Isharp', 'location_footnote_Ssharp', 'location_footnote_Vsharp', 'location_footnote_Wsharp']

footnoteMap = [('location_footnote_Csharp', 'C#'), ('location_footnote_Dsharp', 'D#'), ('location_footnote_Fsharp', 'F#'), ('location_footnote_Hsharp', 'H#'), ('location_footnote_Isharp', 'I#'), ('location_footnote_Ssharp', 'S#'), ('location_footnote_Vsharp', 'V#'), ('location_footnote_Wsharp', 'W#')]

writer = csv.DictWriter(outputFile, fieldnames=fieldnames, delimiter='\t')
writer.writeheader()

class OutputWriter:
    def __init__(self, csvWriter, fields, footnoteMap):
        self.csvWriter = csvWriter
        self.fields = fields
        self.footnoteMap = footnoteMap
    def writeRow(self, inputRow, candidate):
        #extract footnotes, write to spreadsheet
        pass

outputHandler = OutputWriter(writer, fieldnames, footnoteMap)
class Parcel:
    def __init__(self, pid, mblu, location, firstStreet, firstRow, outputHandler):
        self.pid = pid
        self.mblu = mblu
        self.location = location
        self.streets = [firstStreet]
        self.rows = [firstRow]
        self.inputIDList = None
        #map input ID to result
        self.results = {}
        self.outputHandler = outputHandler
    #there are some properties (indicated by property ID) that have duplicate entries, but are marked as being on different streets. 
    def addStreet(self, street, row):
        self.streets.append(street)
        self.rows.append(row)
    def assignInputIDs(self, base):
        #assign an input ID for each street. Each street will be paired with the location and mblu to create a smarty streets lookup
        #return the base + number of streets (the base for the next Parcel)
        assert(self.inputIDList is None)
        assert(len(self.streets) == len(self.rows))
        self.inputIDList = list(range(base, base + len(self.streets)))
        return base + len(self.streets)
    def getLookups(self):
        #return a list of Lookup objects. Split on the street to remove any unit numbers in the location. Use the MBLU to provide secondary. 
        pass
    def writeParcel(self, candidate):
        pass
    def signalParcelInvalid(self):
        pass
    def signalParcelContradiction(self):
        pass
    def finalize(self):
        """
        All of the lookups have results. There are a few scenarios to worry about here:

        1) None of the lookups returned valid candidate. Signal to the user that the address is invalid. 
        2) One lookup returned a valid candidate. Write to the valid output spreadsheet
        3) Multiple lookups returned valid candidates, that are consistent with one another. Arbitrarily pick one and write to spreadsheet
        4) Multiple lookups returned valid candidates, but they are inconsistent with one another. Signal to the user that this is the case. 

        The criteria for consistency is that the delivery point is the same. 
        """
        deliveryPoints = set()
        candidate = None
        for inputID, result in self.results.items():
            if len(result) > 0:
                assert(len(result) == 1)
                candidate = result[0]
                assert(candidate.components.zipcode)
                assert(candidate.components.plus4_code)
                assert(candidate.components.delivery_point >= 0)
                deliveryPoint = str(candidate.components.zipcode) + str(candidate.components.plus4_code) + str(components.delivery_point)
                deliveryPoints.add(deliveryPoint)
        if len(deliveryPoints) == 1:
            self.writeParcel(candidate)
        elif len(deliveryPoints) == 0:
            self.signalParcelInvalid()
        else:
            self.signalParcelContradiction()
    def setResult(self, inputID, result):
        assert(inputID not in self.results)
        self.results[inputID] = result
        if len(self.results) == len(self.inputIDList):
            self.finalize()
parcels = {}
with open(args.inputTSV, 'r') as f:
    reader = csv.DictReader(f, delimiter='\t')
    visionHeaders = reader.fieldnames
    assert('landUse' in visionHeaders)
    for row in reader:
        if row['landUse'].strip().upper() in landUseCodes:
            pid = row['pid'].strip()
            mblu = row['mblu'].strip()
            location = row['location'].strip()
            street = row['street'].strip()
            if pid in parcels:
                assert(mblu.strip() == parcels[pid].mblu)
                assert(location.strip() == parcels[pid].location)
                parcels[pid].addStreet(street, row)
            else:
                parcels[pid] = Parcel(pid, mblu, location, street, row, outputHandler)

base = 0
for pid, parcel in parcels.items():
    base = parcels[pid].assignInputIDs(base)

lookups = []
batchsize=100
creds = StaticCredentials('87f20fb5-a479-c2a6-61fc-f97766549421', 'aPjLRYlSqap2Q30bO5eM')
client = ClientBuilder(creds).build_us_street_api_client()


lookupIter = itertools.chain.from_iterable(map(parcel.items(), lambda pid, parcel: [(pid, lookup) for lookup in parcel.getLookups()]))
for lookupSlice in itertools.islice(lookupIter, batchsize):
    lookupList = list(lookupSlice)
    batch = Batch()
    for pid, lookup in lookupList:
        batch.add(lookup)
    assert(len(batch) == len(lookupList))
    try:
        client.send_batch(batch)
    except exceptions.SmartyException as err:
        print(err)
        assert(False)
    for i, lookup in enumerate(batch):
        pid = lookupList[i][0]
        parcels[pid].setResult(lookup.input_id, lookup.result)
        
"""
What's below this is from previous script version, will remove when finished with this version. 
"""
        
class MBLU:
    def __init__(self, string):
        self.mbluString = string
        parts = string.split('/')
        assert(len(parts) == 5)
        self.map = parts[0].strip()
        self.block = parts[1].strip()
        self.lot = parts[2].strip()
        self.unit = parts[3].strip()

class Query:
    def __init__(self, streetAddress, secondary, city, state, street, record):
        self.streetAddress = streetAddress
        self.secondary = secondary
        self.city = city
        self.state = state
        self.street = street
        self.record = record
    def setCity(self, city):
        self.city = city
    def makeStreetLookup(self, input_id):
        x = StreetLookup()
        x.input_id = input_id
        #Note that the Lookup expects street to be the street address i.e. 39 Butler Street, not just the street (Butler Street)
        x.street = self.streetAddress
        print('street')
        print(x.street)
        if self.secondary:
            x.secondary = self.secondary
        x.city = self.city
        x.state = self.state
        x.match = 'strict'
        return x

def breakLocation(location, street):
    #Extract out any secondary parts of the location, like unit numbers. Return a tuple (primary, secondary)
    assert(street in location)
    streetStartIndex = location.find(street)
    return (location[0:(streetStartIndex + len(street))].strip(), location[(streetStartIndex + len(street))::].strip())


outputFile = open(args.output, 'w')

fieldnames = visionHeaders + ['location_valid', 'city', 'zip', 'plus4_code', 'delivery_point', 'location_components', 'location_footnotes', 'location_metadata', 'location_footnote_Csharp', 'location_footnote_Dsharp', 'location_footnote_Fsharp', 'location_footnote_Hsharp', 'location_footnote_Isharp', 'location_footnote_Ssharp', 'location_footnote_Vsharp', 'location_footnote_Wsharp']

footnoteMap = [('location_footnote_Csharp', 'C#'), ('location_footnote_Dsharp', 'D#'), ('location_footnote_Fsharp', 'F#'), ('location_footnote_Hsharp', 'H#'), ('location_footnote_Isharp', 'I#'), ('location_footnote_Ssharp', 'S#'), ('location_footnote_Vsharp', 'V#'), ('location_footnote_Wsharp', 'W#')]

#fieldnames.extend(['owner_address_valid', 'owner_address_components', 'owner_address_analysis', 'owner_address_metadata', 'owner_address_footnote_Csharp', 'owner_address_footnote_Dsharp', 'owner_address_footnote_Fsharp', 'owner_address_footnote_Hsharp', 'owner_address_footnote_Isharp', 'owner_address_footnote_Ssharp', 'owner_address_footnote_Vsharp', 'owner_address_footnote_Wsharp'])
writer = csv.DictWriter(outputFile, fieldnames=fieldnames, delimiter='\t')
writer.writeheader()

#ownerAddressToComponents = {}

for streetHeadsTuple in recordIter:
    queries = []
    streetHeads = list(streetHeadsTuple)
    while len(streetHeads) > 0 or len(queries) > 0:
        while len(queries) < batchSize and len(streetHeads) > 0:
            head  = streetHeads.pop()
            if head:
                city = cities[streetToCityIndex[head['street']][0]]
                primary, secondary = breakLocation(head['location'], head['street'])
                queries.append(Query(primary, secondary, city, state, head['street'], dict(head)))

        if queries:
            lookups = [queries[i].makeStreetLookup(str(i)) for i in range(0, len(queries))]
            batch = Batch()
            for x in lookups:
                print('lookup')
                print(vars(x))
                batch.add(x)
            #assert(len(batch) == len(lookups))
            try:
                client.send_batch(batch)
            except exceptions.SmartyException as err:
                print(err)
                assert(False)
            for i, lookup in enumerate(batch):
                print('query: ')
                print(vars(queries[i]))
                candidates = lookup.result
                print('candidates')
                print(candidates)
                street = queries[i].street
                cityIndex, numTriesRemaining = streetToCityIndex[street]
                if len(candidates) == 0:
                    if numTriesRemaining == 0:
                        streetToCityIndex[street] = (0, numCities)
                        #Invalid address. Place into spreadsheet
                        record = queries[i].record
                        record['location_valid'] = 'INVALID'
                        writer.writerow(record)
                        queries[i] = None
                    else:
                        cityIndex = (cityIndex + 1) % numCities
                        streetToCityIndex[street] = (cityIndex, numTriesRemaining - 1)
                        queries[i].city = cities[cityIndex]
                else:
                    streetToCityIndex[street] = (cityIndex, numCities)
                    components = candidates[0].components
                    metadata = candidates[0].metadata
                    analysis = candidates[0].analysis
                    #TODO Valid address. Place necessary information into spreadsheet.
                    record = queries[i].record
                    record['location_valid'] = 'VALID'
                    record['city'] = components.city_name
                    record['zip'] = components.zipcode
                    record['plus4_code'] = str(components.plus4_code)
                    record['delivery_point'] = components.delivery_point
                    record['location_components'] = json.dumps(vars(components))
                    record['location_metadata'] = json.dumps(vars(metadata))
                    record['location_footnotes'] = analysis.footnotes
                    for field, footnote in footnoteMap:                        
                        if analysis.footnotes and footnote in analysis.footnotes:
                            record[field] = 'TRUE'
                    writer.writerow(record)
                    queries[i] = None                    
                    
            queries = [i for i in queries if i]                      
            
        
