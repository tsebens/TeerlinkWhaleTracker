# Add Vessel name to table
import csv

def getData( fp ):
	with open( fp ) as f:
		reader = csv.DictReader( f )
		data = [l for l in reader]
	return data
	
def getVesselName( PhotoLabel ):
	vesselDict = { 'MA':'MARINER', 'SE':'SEEKER', 'SO':'SOUNDER', 'EX':'EXPLORER', 'NA':'NAVIGATOR', 'UN':'UNKNOWN' }
	vessel = vesselDict[PhotoLabel[13:15]]
	return vessel
	
	
def main():
	fp = r"C:\Users\tristan.sebens\Documents\Code\GGC-AIS\Group-Whale Link.csv"
	out_fp = r"C:\Users\tristan.sebens\Documents\Code\GGC-AIS\output.csv"
	data = getData( fp )
	for d in data:
		d['Vessel'] = getVesselName( d['Photo_Label'] )
	header = list( data[0].keys() )
	with open( out_fp, 'w', newline='' ) as f:
		writer = csv.writer( f )
		writer.writerow( header )
		del writer
		writer = csv.DictWriter( f, fieldnames=header )
		writer.writerows( data )
	
main()