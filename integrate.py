# Integrate timestamp from photos into GGC collected data

import sys
import os
import os.path
import csv
import exifread
from date_and_time import Eastern, Central, Mountain, Pacific, Alaska, UTC
from datetime import datetime

# Retrieve tzinfo object so that we can perform intelligent and aware time comparisons.
# This is most likely overkill, but it does make our process more robust.
tzs = (Eastern, Central, Mountain, Pacific, Alaska, UTC)

cwd = os.getcwd()
photoDir = os.path.join( cwd, "2014 GGC photos" )
csvDataFp = os.path.join( cwd, "GGC Data Files", "Group-Whale Link.csv" )

class UnknownTimeZone(Exception):
	pass
	
# Reads the csv data contained in the file at the passed fp, and returns it as a list of dict objects.
def getCSVData( data_fp ):
	with open( data_fp, 'r' ) as f:
		dReader = csv.DictReader( f, dialect='excel' )
		data = [l for l in dReader]
	return data
	
# Comb a directory for photos, read the timestamp from each photo, and for each photo found return a dict containing that photo's label, and it's timestamp.
def getPhotoData( ph_dir ):
	photo_fps = [os.path.join( ph_dir, f ) for f in os.listdir( ph_dir )]
	ph_timestamp_dict = {}
	for ph in photo_fps:
		ph_name = os.path.splitext( os.path.basename( ph ) )[0]
		with open( ph, 'rb' ) as ph_file:
			tags = exifread.process_file( ph_file )
			dir, name = os.path.split( ph )
			key, ext = os.path.splitext( name )
			key = key.rstrip()
			date, time = str( tags['EXIF DateTimeOriginal'] ).split( ' ' )
			entry = {'Photo_Label':ph_name, 'date':date, 'time':time}
			ph_timestamp_dict[key] = entry
	return ph_timestamp_dict

# Returns the header (fields) of a dict-written csv file as a list
def getDictHeader( dict ):
	dict_keys = list( dict.keys() )
	first_key = dict_keys[0]
	dict_header = list( dict[first_key].keys() )
	return dict_header
	
# Accepts two list of dict objects representing the ggc data and the photo timestamp data, and performs an inner join using the photo_label as the primary key. Writes the resultant data to the passed output_fp
def joinDictData( data_dict, ph_timestamp_dict, output_fp ):
	for row in data_dict:
		ph_label = row['Photo_Label']
		# Remove any potential spaces from the back of the key
		ph_label = ph_label.rstrip()
		ph_dict_entry = ph_timestamp_dict[ph_label] # Retrieve the timestamp from the ph_timestamp_dict using the photo label
		date = ph_dict_entry['date']
		time = ph_dict_entry['time']
		row['PhotoTimeStamp'] = time
		row['PhotoDateStamp'] = date
	return( data_dict )
		
# Records a dict object as a csv file with a header.
def recordDict( dict, csv_fp ):
	header = list( dict[0].keys() ) # Get the keys from the first dict in the list
	# Add header to new data file
	with open( csv_fp, 'w' ) as ggc_data_file:
		writer = csv.writer( ggc_data_file )
		writer.writerow( header )	# Now we begin writing the data to the output file
	with open( csv_fp, 'a', newline='' ) as ggc_data_file:	
		writer = csv.DictWriter( ggc_data_file, header )
		# For each dict in the data list, 
		for row in dict:
			writer.writerow( row )

def getTzInfo( n ):
	for tz in tzs:
		if tz.stdname == n:
			return tz
	raise UnknownTimeZone("Unknown timezone cited", n)

# Returns a month as an int. May through key error if the month isn't recognized.	
def getMonthAsInt( mon ):
	mon = mon.lower()
	m_dict = {
		'jan': 1,
		'feb': 2,
		'mar': 3,
		'apr': 4,
		'may': 5,
		'jun': 6,
		'jul': 7,
		'aug': 8,
		'sep': 9,
		'oct': 10,
		'nov': 11,
		'dec': 12	
	}
	return m_dict[mon]

# Accepts an AIS 'Base station time stamp' string, and returns a tz aware date time object
def getAISDateTimeInfo( ais_timestamp ):
	#Template: 01 May 2014 18:54:40.480 UTC
	time_dict = {}
	elems = ais_timestamp.split( ' ' )\
	# First we grab the calendar info
	time_dict['day'] = int( elems[0] )
	time_dict['month'] = getMonthAsInt( elems[1] )
	time_dict['year'] = int( elems[2] )
	time_dict['tz'] = elems[4]
	# Then we get the clock info
	time = elems[3]
	t_elems = time.split( ':' )
	time_dict['hour'] = int( t_elems[0] )
	time_dict['minute'] = int( t_elems[1] )
	time_dict['second'] = int( float( t_elems[2] ) ) # Round to an int because it's not worth the trouble to maintain greater precision
	tz = getTzInfo( time_dict['tz'] )
	return datetime( time_dict['year'], time_dict['month'], time_dict['day'], hour=time_dict['hour'], second=time_dict['second'], tzinfo=tz )
			
def getAISData( ais_dir ):
	# Gather all available AIS data files
	ais_fps = [os.path.join( ais_dir, f ) for f in os.listdir( ais_dir )]
	sum_data = list()
	for ais_fp in ais_fps:
		ais_data = getCSVData( ais_fp )
		sum_data.extend( ais_data )
	return sum_data
	
def main():
	output_fp = os.path.join( cwd, 'output.csv' )
	ph_timestamp_dict = getPhotoData( photoDir )
	ggc_dict = getCSVData( csvDataFp )
	data_dict = joinDictData( ggc_dict, ph_timestamp_dict, output_fp )
	recordDict( data_dict, output_fp )
		
if __name__ == '__main__':
	main()