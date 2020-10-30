#!/usr/bin/python
from __future__ import print_function
import sys
import rubrik_cdm
import re
import getopt
import getpass
import datetime
import pytz
import requests
import urllib3
urllib3.disable_warnings()

def usage():
    print ("Usage Goes here!")
    exit(0)

def dprint(message):
    if DEBUG:
        print(message)

def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")

def python_input (message):
    if int(sys.version[0]) > 2:
        in_val = input(message)
    else:
        in_val = raw_input(message)
    return (in_val)

def convert_time(mtime, local_zone):
    mtime = mtime[:-5]
    mtime_dt = datetime.datetime.strptime(mtime, '%Y-%m-%dT%H:%M:%S')
    mtime_dt_s = pytz.utc.localize(mtime_dt).astimezone(local_zone)
    return(str(mtime_dt_s)[:-6])

def get_snap_info(rubrik, snap_id, local_zone):
    snap = rubrik.get('v1', '/fileset/snapshot/' + snap_id)
    print("SNAP_DATE = " + str(snap['date']))
    stime = convert_time(snap['date'], local_zone)
    return((stime, bool(snap['cloudState'])))

def get_latest_mtime(file_data):
    latest_dt = datetime.datetime.fromtimestamp(0)
    print(file_data)
    for s in file_data['data'][0]['fileVersions']:
        mtime_dt = datetime.datetime.strptime(s['lastModified'][:-5], '%Y-%m-%dT%H:%M:%S')
        if mtime_dt > latest_dt:
            latest_dt = mtime_dt
    return (latest_dt)

def get_backup_date(rubrik, snap_id):
    if not snap_id in snap_date_cache:
        date_s = rubrik.get('v1', '/fileset/snapshot/' + str(snap_id), timeout=60)
        date_dt = datetime.datetime.strptime(date_s['date'][:-5], '%Y-%m-%dT%H:%M:%S')
        date_dt_s = pytz.utc.localize(date_dt).astimezone(local_zone)
        snap_date_cache[snap_id] = str(date_dt_s)[:-6]
    return(snap_date_cache[snap_id])

def rubrik_get(rubrik_host, api_call, user, password):
    resp_list = []
    api_call = requests.utils.requote_uri(api_call)
    url = "https://" + rubrik_host + "/api" + api_call
    more = True
    while more:
        dprint("URL: " + url)
        resp = requests.get(url, verify=False, auth=(user, password)).json()
        resp_list.append(resp)
        more = resp['hasMore']
        if more:
            url = "https://" + rubrik_host + "/api" + api_call + "&cursor=" + resp['nextCursor']
    return(resp_list)

def validate_fields(fields_s):
    fields = fields_s.split(',')
    for f in fields:
        if f not in ['name', 'size', 'mtime', 'backup', 'type', 'location']:
            sys.stderr.write("Invalid field: " + f + '\n')
            fields.remove(f)
    return(fields)

def print_file(file, index, fields, delim):
    line = ""
    first = True
    for f in fields:
        if not first:
            line += delim
        if f == "name":
            line += file['path']
        elif f == "size":
            line += str(file['fileVersions'][index]['size'])
        elif f == "mtime":
            mtime = file['fileVersions'][index]['lastModified'][:-5]
            mtime_dt = datetime.datetime.strptime(mtime, '%Y-%m-%dT%H:%M:%S')
            mtime_local = pytz.utc.localize(mtime_dt).astimezone(local_zone)
            line += str(mtime_local)[:-6]
        elif f == "backup":
            backup_date = get_backup_date(rubrik, file['fileVersions'][index]['snapshotId'])
            line += backup_date
        elif f == "type":
            line += file['fileVersions'][index]['fileMode']
        elif f == "location":
            if file['fileVersions'][index]['source'] == "cloud":
                line += "Archive"
            else:
                line += "Local"
        first = False
    print (line)

if __name__ == "__main__":
    backup = ""
    rubrik_node = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    DEBUG = False
    latest = True
    physical = False
    share_id = ""
    snap_list = []
    search_results = []
    snap_date_cache = {}
    outfile = ""
    fh = ""
    share = ""
    name = "*"
    delim = ","
    fields = ['name', 'size', 'mtime', 'backup']

    optlist, args = getopt.getopt(sys.argv[1:], 'hDlc:n:b:f:F:d:', ['--help', '--DEBUG', '--creds=', '--name=', '--backup=', '--fileset=', '--format=', '--delim'])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-n', '--name'):
            name = a
        if opt in ('-b', '--backup'):
            backup = a
        if opt in ('-f', '--fileset'):
            fileset = a
        if opt in ('-F', '--format'):
            fields = validate_fields(a)
        if opt in ('-d', '--delim'):
            delim = a

        try:
            rubrik_node = args[0]
        except:
            usage()

    if not user:
        user = python_input ("User: ")
    if not password:
        password = getpass.getpass("Password: ")
    if not backup:
        backup = python_input("Backup: [host | host:share]: ")

    if ':' in backup:
        (host, share) = backup.split(':')
    else:
        physical = True
        host = backup
    rubrik = rubrik_cdm.Connect (rubrik_node, user, password)
    rubrik_config = rubrik.get('v1', '/cluster/me', timeout=60)
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utz_zone = pytz.timezone('utc')
    if not physical:
        hs_data = rubrik.get('internal', '/host/share', timeout=60)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = str(x['id'])
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit (2)
        fs_data = rubrik.get('v1', '/fileset?share_id=' + share_id + "&name=" + fileset, timeout=60)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=60)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        if share_id == "":
            sys.stderr.write("Host not found\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=60)
    dprint("SH_ID: " + str(share_id))
    fs_id = ""
    if fileset:
        for fs in fs_data['data']:
            if fs['name'] == fileset:
                fs_id = fs['id']
                break
        if fs_id == "":
            sys.stderr.write("Fileset not found\n")
            exit(2)
    else:
        if fs_data['total'] == 1:
            fs_id = fs_data['data'][0]['id']
        else:
            fs_list = []
            for i, fs in enumerate(fs_data['data']):
                fs_list.append(fs['id'])
                print(str(i) + ": " + fs['name'])
            done = False
            while not done:
                fs_index = python_input ("Select Fileset: ")
                if int(fs_index) >= 0 and int(fs_index) < int(fs_data['total']):
                    done = True
                else:
                    print("Invalid Entry")
            fs_id = fs_data['data'][int(fs_index)]['id']
    dprint("FS_ID: " + str(fs_id))
    api_call = "/internal/search?managed_id=" + fs_id + "&query_string=" + name
    search_results = rubrik_get(rubrik_node, api_call, user, password)
    print(search_results)
    for search_part in search_results:
        if latest:
            index = -1
        else:
            index = 0 # Replace with code if needed.  Just a filler
        for file in search_part['data']:
            print_file(file, index, fields, delim)