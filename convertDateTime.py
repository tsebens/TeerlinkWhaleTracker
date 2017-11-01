# Convert Dates in aggregate_AIS to SQLite datetime format
import os
import os.path
import csv

month_dict = { 'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12' }

def convertAISDatetime( datetime_stamp ):
	year = datetime_stamp[7:11]
	month_key = datetime_stamp[3:6]
	month = month_dict[month_key]
	day = datetime_stamp[:2]
	hour = datetime_stamp[12:14]
	minute = datetime_stamp[15:17]
	second = datetime_stamp[18:20]
	return "%s-%s-%s %s:%s:%s" % ( year, month, day, hour, minute, second )

agg_fp = r"C:\Users\tristan.sebens\Documents\Code\GGC-AIS\AIS Aggregate\aggregate_AIS.csv"
out_fp = r"C:\Users\tristan.sebens\Documents\Code\GGC-AIS\AIS Aggregate\out.csv"
with open( agg_fp ) as f, open( out_fp, 'w', newline='' ) as o_f:
	reader = csv.DictReader( f )
	writer = csv.DictWriter( o_f, fieldnames=( 'ROWID', 'conv_timestamp' ) )
	for row in reader:
		timestamp = row['Base station time stamp']
		row_id = row['ROWID']
		conv_timestamp = convertAISDatetime( timestamp )
		new_row = { 'ROWID':row_id, 'conv_timestamp':conv_timestamp	}
		writer.writerow( new_row )
	