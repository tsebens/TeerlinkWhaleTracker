# Check GGC data for discrepancies between photo timestamp and manually recorded date/time

import csv

def exportToCSV( in_fp, out_fp ):
	with open( in_fp, 'r' ) as in_f, open( out_fp, 'w' ) as out_f:
		reader = csv.reader( in_f, dialect='excel' )
		header = reader.__next__()
		writer = csv.writer( out_f, delimiter=',' )
		writer.writerow( header )
		writer.writerows( reader )

