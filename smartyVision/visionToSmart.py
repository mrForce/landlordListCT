import csv
import sys
import argparse
import collections
import itertools
import json
from smartystreets_python_sdk import StaticCredentials, exceptions, Batch, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup as StreetLookup



parser = argparse.ArgumentParser(description='Take output from Vision GIS, run building address and owner address through SmartyStreets')
parser.add_argument('inputTSV', help='Output from Vision script')
parser.add_argument('city', help='City')
parser.add_argument('state', help='State')
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
state = args.state
city = args.city
landUseCodes = set(extractLines(args.landuse))
visionHeaders = None

with open(args.inputTSV, 'r') as f:
    reader = csv.DictReader(f, delimiter='\t')
    visionHeaders = reader.fieldnames
    
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
    def writeRow(self, inputRow, candidate, valid):
        #extract footnotes, write to spreadsheet
        if valid:
            components = candidate.components
            metadata = candidate.metadata
            analysis = candidate.analysis
            inputRow['location_valid'] = 'VALID'
            inputRow['city'] = components.city_name
            inputRow['zip'] = components.zipcode
            inputRow['plus4_code'] = str(components.plus4_code)
            inputRow['delivery_point'] = components.delivery_point
            inputRow['location_components'] = json.dumps(vars(components))
            inputRow['location_metadata'] = json.dumps(vars(metadata))
            inputRow['location_footnotes'] = analysis.footnotes
            for field, footnote in self.footnoteMap:                        
                if analysis.footnotes and footnote in analysis.footnotes:
                    inputRow[field] = 'TRUE'
        else:
            inputRow['location_valid'] = 'INVALID'
        self.csvWriter.writerow(inputRow)
outputHandler = OutputWriter(writer, fieldnames, footnoteMap)
class Parcel:
    def __init__(self, pid, mblu, location, city, state, firstStreet, firstRow, outputHandler):
        self.pid = pid
        self.mblu = mblu
        self.city = city
        self.state = state
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
        self.inputIDList = [str(x) for x in range(base, base + len(self.streets))]
        return base + len(self.streets)
    def getLookups(self):
        #return a list of Lookup objects. Split on the street to remove any unit numbers in the location. Use the MBLU to provide secondary.        
        assert(len(self.inputIDList) == len(self.streets))
        assert(len(self.streets) == len(self.rows))
        lookups = []
        for i in range(0, len(self.rows)):
            x = StreetLookup()
            x.input_id = str(self.inputIDList[i])
            x.match = 'strict'
            assert(self.streets[i] in self.location)
            streetStartIndex = self.location.find(self.streets[i])
            x.street = self.location[0:(streetStartIndex + len(self.streets[i]))].strip()
            mbluParts = self.mblu.split('/')
            assert(len(mbluParts) == 5)
            unit = mbluParts[3].strip()
            if unit:
                x.secondary = unit
            x.city = self.city
            x.state = self.state
            lookups.append(x)
        return lookups
    def writeParcel(self, candidateRow, candidate, valid):
        self.outputHandler.writeRow(candidateRow, candidate, valid)
    def signalParcelInvalid(self):
        sys.stderr.write('Parcel with PID: {}, location: {} and streets: {} was invalid\n'.format(self.pid, self.location, str(self.streets)))
    def signalParcelContradiction(self):
        sys.stderr.write('Parcel with PID: {}, location: {} and streets: {} had contradictory candidates. Here are the streets and their corresponding lookups: \n'.format(self.pid, self.location, str(self.streets)))
        for inputID, completedLookup in self.results.items():
            result = completedLookup.result
            if len(result) > 0:
                assert(len(result) == 1)
                sys.stderr.write('Street: {}, lookup: {}\n'.format(str(self.streets[self.inputIDList.index(inputID)]), str(vars(completedLookup))))

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
        candidateRow = None
        for inputID, completedLookup in self.results.items():
            result = completedLookup.result
            if len(result) > 0:
                assert(len(result) == 1)
                candidate = result[0]
                candidateRow = self.rows[self.inputIDList.index(inputID)]
                assert(candidate.components.zipcode)
                assert(candidate.components.plus4_code)
                assert(candidate.components.delivery_point)
                deliveryPoint = str(candidate.components.zipcode) + str(candidate.components.plus4_code) + str(candidate.components.delivery_point)
                deliveryPoints.add(deliveryPoint)
        if len(deliveryPoints) == 1:
            self.writeParcel(candidateRow, candidate, True)
        elif len(deliveryPoints) == 0:
            for x in self.rows:                
                self.writeParcel(x, None, False)
            self.signalParcelInvalid()
        else:
            self.signalParcelContradiction()
    def setResult(self, inputID, lookup):
        assert(inputID not in self.results)
        self.results[inputID] = lookup
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
                if location.strip() != parcels[pid].location:
                    print('location: ' + location.strip())
                    print(parcels[pid].location)
                assert(location.strip() == parcels[pid].location)
                parcels[pid].addStreet(street, row)
            else:
                parcels[pid] = Parcel(pid, mblu, location, city, state, street, row, outputHandler)

base = 0
for pid, parcel in parcels.items():
    base = parcels[pid].assignInputIDs(base)

lookups = []
batchsize=100
creds = StaticCredentials('87f20fb5-a479-c2a6-61fc-f97766549421', 'aPjLRYlSqap2Q30bO5eM')
client = ClientBuilder(creds).build_us_street_api_client()


lookupIter = itertools.chain.from_iterable(map(lambda pidAndParcel: [(pidAndParcel[0], lookup) for lookup in pidAndParcel[1].getLookups()], parcels.items()))
while True:
    lookupList = list(itertools.islice(lookupIter, batchsize))
    if len(lookupList) == 0:
        break
    print(lookupList)
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
        parcels[pid].setResult(lookup.input_id, lookup)
