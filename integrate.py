# Integrate timestamp from photos into GGC collected data
# Assumes that every photo timestamp is in AK timezone format. A pretty reasonable assumption, given the circumstances, but an assumption nonetheless

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
aisDir = os.path.join( cwd, "AIS Data" )
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
			datetime = getPhotoDateTimeInfo( str( tags['EXIF DateTimeOriginal'] ) )
			entry = {'Photo_Label':ph_name, 'DateTime': datetime}
			ph_timestamp_dict[key] = entry
	return ph_timestamp_dict

# Returns the header (fields) of a dict-written csv file as a list
def getDictHeader( dict ):
	dict_keys = list( dict.keys() )
	first_key = dict_keys[0]
	dict_header = list( dict[first_key].keys() )
	return dict_header
	
# Accepts two list of dict objects representing the ggc data and the photo timestamp data, and performs an inner join using the photo_label as the primary key. Writes the resultant data to the passed output_fp
def joinDictData( data_dict, ph_timestamp_dict ):
	for row in data_dict:
		ph_label = row['Photo_Label']
		# Remove any potential spaces from the back of the key
		ph_label = ph_label.rstrip()
		ph_dict_entry = ph_timestamp_dict[ph_label] # Retrieve the timestamp from the ph_timestamp_dict using the photo label
		row['PhotoDateTime'] = ph_dict_entry['DateTime']
	return data_dict
		
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

# Returns a datetime object representing the date and time when the photo was taken
def getPhotoDateTimeInfo( ph_datetime ):
	date, time = ph_datetime.split( ' ' )
	year, month, day = [int( e ) for e in date.split( ':' )]
	hour, minute, second = [int( e ) for e in time.split( ':' )]
	return datetime( year, month, day, hour, minute, second, tzinfo=getTzInfo( 'AKT' ) ) # We are assuming that the photo timestamp is recorded in AKT
	
# Accepts an AIS 'Base station time stamp' string, and returns a tz aware date time object
# This function is shit. Rewrite it.
def getAISDateTimeInfo( ais_timestamp ):
	#Template: 01 May 2014 18:54:40.480 UTC
	time_dict = {}
	elems = ais_timestamp.split( ' ' )
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
	return datetime( time_dict['year'], time_dict['month'], time_dict['day'], hour=time_dict['hour'], minute=time_dict['minute'], second=time_dict['second'], tzinfo=tz )
			
def getAISData( ais_dir ):
	# Gather all available AIS data files
	ais_fps = [os.path.join( ais_dir, f ) for f in os.listdir( ais_dir )]
	ais_data = list()
	# For now we'll just create one enormous dictionary of AIS data that we'll sift through. Computationally expensive, but fuck it. It's easier.
	for ais_fp in ais_fps:
		new_data = getCSVData( ais_fp )
		ais_data.extend( new_data )
	# Now that we have all of the AIS data points, we need to create datetime objects for each one.
	for entry in ais_data:
		ts = entry['Base station time stamp']
		dt = getAISDateTimeInfo( ts )
		entry['DateTime'] = dt
	return ais_data
	
def getClosestAISEntry( data_entry, ais_dict, starting_index=0 ):
	data_point_timestamp = data_entry['PhotoDateTime']
	closest_entry = ais_dict[starting_index]
	shortest_distance = abs( ais_dict[0]['DateTime'] - data_point_timestamp )# This is not a simple data type, it is a timedelta object.
	index = starting_index + 1
	for ais_entry in ais_dict[index:]: # Skip the first entry because we've already taken that one as our initial best answer
		new_distance = abs( ais_entry['DateTime'] - data_point_timestamp )
		#print( 'CD: %s - ND: %s' % (shortest_distance, new_distance) )
		if new_distance <= shortest_distance:
			closest_entry = ais_entry
			shortest_distance = new_distance
			index += 1
		else:
			#print( 'CE: %s\nD: %s' % (closest_entry, shortest_distance) ) 
			return closest_entry, index

def getFirstAndLastAISEntry( ais_dict ):
	return ( min( ais_dict, key=lambda entry: entry['DateTime'] ), max( ais_dict, key=lambda entry: entry['DateTime'] ) )
	
def getFirstAndLastGGCEntry( data_dict ):
	return ( min( data_dict, key=lambda entry:['PhotoDateTime'] ), max( data_dict, key=lambda entry:['PhotoDateTime'] ) )
		
def main():
	output_fp = os.path.join( cwd, 'output.csv' )
	ph_timestamp_dict = getPhotoData( photoDir )
	print( 'Loading GGC Data...' )
	ggc_dict = getCSVData( csvDataFp )
	data_dict = joinDictData( ggc_dict, ph_timestamp_dict )
	print( 'Loading AIS Data...' )
	ais_dict = getAISData( aisDir )
	print( 'Found %s AIS entries.' % len( ais_dict ) )
	# Since there are so many entries, and since we're going to be doing a lot of lookups on this data, it behooves us to sort it.
	print( 'Sourting GGC Data...' )
	data_dict.sort(key = lambda entry:entry['PhotoDateTime'])
	print( 'Sorting AIS Data...' )
	ais_dict.sort(key=lambda entry:entry['DateTime'])
	print( 'Sorting complete.' )
	print( 'Matching Photo timestamps to AIS data...' )
	count = 0
	last_index = 0
	for entry in data_dict:
		closest_entry, new_index = getClosestAISEntry( entry, ais_dict, last_index )
		entry['Latitude'] = closest_entry['Latitude']
		entry['Longitude'] = closest_entry['Longitude']
		entry['AISMatchDistance'] = abs( closest_entry['DateTime'] - entry['PhotoDateTime'] )
		print( 'Points matched: %s - LMD: %s - LMI: %s - Is Greater: %s' % ( count, abs( entry['PhotoDateTime'] - closest_entry['DateTime'] ), new_index, new_index > last_index ) )
		last_index = new_index - 1000
		if last_index < 0:
			last_index = 0
		count += 1
		
	print( '' )	
	print( 'Matching complete. Recording results.' )
	recordDict( data_dict, output_fp )
		
if __name__ == '__main__':
	main()