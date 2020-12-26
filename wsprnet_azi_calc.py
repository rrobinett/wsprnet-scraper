# -*- coding: utf-8 -*-
# Filename: wsprnet_azi_calc.py
# February  2020  Gwyn Griffiths

# Take a scraper latest_log.txt file, extract the receiver and transmitter Maidenhead locators and calculates azimuth at tx and rx in that order
# Needs one argument, file path for latest_log.txt
# V1.0 outputs the azi-appended data to a file spots+azi.csv that is overwritten next 3 min cycle, this file appended to the master
# V1.1 also outputs lat and lo for tx and rx and the vertex, the point on the path nearest the pole (highest latitude) for the short path
# The vertex in the other hemisphere has the sign of the latitude reversed and 180˚ added to the longitude
# V1.2 The operating band is derived from the frequency, 60 and 60eu and 80 and 80eu are reported as 60 and 80
# Miles are not copied to the azi-appended file
# In the script the following lines preceed this code and there's an EOF added at the end
# V1.3 RR modified to accept API spot lines

import argparse
import csv
import sys
import numpy as np

freq_to_band = {
    1: 2200,
    4: 630,
    18: 160,
    35: 80,
    52: 60,
    53: 60,
    70: 40,
    101: 30,
    140: 20,
    181: 17,
    210: 15,
    249: 12,
    281: 10,
    502: 6,
    700: 4,
    1444: 2,
    4323: 70,
    12965: 23
}
default_band = 9999

# define function to convert 4 or 6 character Maidenhead locator to lat and lon in degrees
def loc_to_lat_lon(locator):
    locator=locator.strip()
    decomp=list(locator)
    lat=(((ord(decomp[1])-65)*10)+(ord(decomp[3])-48)+(1/2)-90)
    lon=(((ord(decomp[0])-65)*20)+((ord(decomp[2])-48)*2)+(1)-180)
    if len(locator)==6:
        if (ord(decomp[4])) >88:    # check for case of the third pair, likely to  be lower case
            ascii_base=96
        else:
            ascii_base=64
        lat=lat-(1/2)+((ord(decomp[5])-ascii_base)/24)-(1/48)
        lon=lon-(1)+((ord(decomp[4])-ascii_base)/12)-(1/24)
    return(lat, lon)

def calculate_azimuth(frequency, tx_locator, rx_locator):
    (tx_lat, tx_lon) = loc_to_lat_lon(tx_locator)    # call function to do conversion, then convert to radians
    phi_tx_lat = np.radians(tx_lat)
    lambda_tx_lon = np.radians(tx_lon)
    (rx_lat,rx_lon) = loc_to_lat_lon(rx_locator)    # call function to do conversion, then convert to radians
    phi_rx_lat = np.radians(rx_lat)
    lambda_rx_lon = np.radians(rx_lon)
    delta_phi = (phi_tx_lat - phi_rx_lat)
    delta_lambda = (lambda_tx_lon-lambda_rx_lon)

    # calculate azimuth at the rx
    y = np.sin(delta_lambda) * np.cos(phi_tx_lat)
    x = np.cos(phi_rx_lat)*np.sin(phi_tx_lat) - np.sin(phi_rx_lat)*np.cos(phi_tx_lat)*np.cos(delta_lambda)
    rx_azi = (np.degrees(np.arctan2(y, x))) % 360

    # calculate azimuth at the tx
    p = np.sin(-delta_lambda) * np.cos(phi_rx_lat)
    q = np.cos(phi_tx_lat)*np.sin(phi_rx_lat) - np.sin(phi_tx_lat)*np.cos(phi_rx_lat)*np.cos(-delta_lambda)
    tx_azi = (np.degrees(np.arctan2(p, q))) % 360

    # calculate the vertex, the lat lon at the point on the great circle path nearest the nearest pole, this is the highest latitude on the path
    # no need to calculate special case of both transmitter and receiver on the equator, is handled OK
    # Need special case for any meridian, where vertex longitude is the meridian longitude and the vertex latitude is the lat nearest the N or S pole
    if tx_lon == rx_lon:
        v_lon = tx_lon
        v_lat = max([tx_lat, rx_lat], key=abs)
    else:
        v_lat = np.degrees(np.arccos(np.sin(np.radians(rx_azi))*np.cos(phi_rx_lat)))
    if v_lat > 90.0:
        v_lat = 180 - v_lat
    if rx_azi < 180:
        v_lon = ((rx_lon + np.degrees(np.arccos(np.tan(phi_rx_lat) / np.tan(np.radians(v_lat))))) + 360) % 360
    else:
        v_lon = ((rx_lon - np.degrees(np.arccos(np.tan(phi_rx_lat) / np.tan(np.radians(v_lat))))) + 360) % 360
    if v_lon > 180:
        v_lon = -(360 - v_lon)
    # now test if vertex is not  on great circle track, if so, lat/lon nearest pole is used
    if v_lon < min(tx_lon, rx_lon) or v_lon > max(tx_lon, rx_lon):
    # this is the off track case
        v_lat = max([tx_lat, rx_lat], key=abs)
        if v_lat == tx_lat:
            v_lon = tx_lon
        else:
            v_lon = rx_lon
    # derive the band in metres (except 70cm and 23cm reported as 70 and 23) from the frequency
    freq = int(10 * float(frequency))
    band = freq_to_band.get(freq, default_band)
    return (band, rx_azi, rx_lat, rx_lon, tx_azi, tx_lat, tx_lon, v_lat, v_lon)

def wsprnet_azi_calc(input_path, output_file):
    # now read in lines file, as a single string, skip over lines with unexpected number of columns
    spot_lines=np.genfromtxt(input_path, dtype='str', delimiter=',', loose=True, invalid_raise=False)
    # get number of lines
    n_lines=len(spot_lines)

    # loop to calculate  azimuths at tx and rx (wsprnet only does the tx azimuth)
    spots = []
    for i in range(0, n_lines):
        (band, rx_azi, rx_lat, rx_lon, tx_azi, tx_lat, tx_lon, v_lat, v_lon) = calculate_azimuth(frequency=spot_lines[i, 6], tx_locator=spot_lines[i, 8], rx_locator=spot_lines[i, 4])
        # output the original data and add lat lon at tx and rx, azi at tx and rx, vertex lat lon and the band
        spots.append([
            spot_lines[i, 0],
            spot_lines[i, 1],
            spot_lines[i, 2],
            spot_lines[i, 3],
            spot_lines[i, 4],
            spot_lines[i, 5],
            spot_lines[i, 6],
            spot_lines[i, 7],
            spot_lines[i, 8],
            spot_lines[i, 9],
            spot_lines[i, 10],
            spot_lines[i, 11],
            spot_lines[i, 12],
            spot_lines[i, 13],
            spot_lines[i, 14],
            spot_lines[i, 15],
            band,
            "-999.9",
            "-999.9",
            int(round(rx_azi)),
            "%.3f" % (rx_lat),
            "%.3f" % (rx_lon),
            int(round(tx_azi)),
            "%.3f" % (tx_lat),
            "%.3f" % (tx_lon),
            "%.3f" % (v_lat),
            "%.3f" % (v_lon)
        ])

    # open file for output as a csv file, to which we will copy original data and the tx and rx azimuths
    with output_file as out_file:
        out_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for spot in spots:
            out_writer.writerow(spot)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add azimuth calculations to a WSPRNET Spots TSV file')
    parser.add_argument("-i", "--input", dest="spotsFile", help="FILE is a CSV containing WSPRNET spots", metavar="FILE", required=True, type=str) # type=argparse.FileType('r')
    parser.add_argument("-o", "--output", dest="spotsPlusAzimuthsFile", help="FILE is a CSV containing WSPRNET spots", metavar="FILE", required=True, nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()

    wsprnet_azi_calc(input_path=args.spotsFile, output_file=args.spotsPlusAzimuthsFile)