# Add in GPS data to Photos based on dates, timestamps, and boat names

import csv

def getPhotoData( fp ):
	with open( fp ) as f:
		dReader = csv.DictReader( f, delimiter=',' )
		data = [r for r in dReader]
		del dReader
	return data
	
def getAISData( fp ):
	pass

def main():	
	fp = r"C:\Users\tristan.sebens\Documents\Code\GGC-AIS\test_output.csv"
	data = getPhotoData( fp )
	

