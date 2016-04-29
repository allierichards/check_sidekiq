#!/usr/bin/env python

import sys
import argparse
import urllib2
import json
import time

# defaults
noise = 0 
crit = 0
warn = 5
sleeptime = 30

# Nagios exit codes
STATE_OK = 0
STATE_WARN = 1
STATE_CRIT = 2
STATE_UNKNOWN = 3

def get_queue_sizes():
  try: response = urllib2.urlopen(queue_stats_url)
  except urllib2.URLError as e:
    exit_unknown({'msg': e})
  except urllib2.HTTPError as e:
    exit_unknown({'msg': e})
  data = response.read()
  try: json_str = json.loads(data)
  except ValueError as e:
    exit_unknown({'msg': e})
  return json_str

def exit_ok(dict):
  print "OK: {}".format(dict['msg'])
  sys.exit(STATE_OK)

def exit_warn(dict):
  print "WARNING: Sidekiq queue processing rates are too slow\n{}".format(dict['msg'])
  sys.exit(STATE_WARN)

def exit_crit(dict):
  print "CRITICAL: Sidekiq queue processing rates are too slow\n{}".format(dict['msg'])
  sys.exit(STATE_CRIT)

def exit_unknown(dict):
  print "UNKNOWN: {}".format(dict['msg'])
  sys.exit(STATE_UNKNOWN)

# this is an ugly hack, but it works
# we want to specify the URL on the command line *before* parsing with argparse
# we need to do this because we won't know what arguments to tell argparse about
# until after the URL is fetched
url_count = 0
my_args = []
for x in range(1,len(sys.argv)):
  if sys.argv[x][:4] == "http":
    url_count += 1
    sidekiq_web_ui = sys.argv[x]
    if url_count > 1:
      exit_unknown({'msg': 'More than one URL specified'})
  else:
    my_args.append(sys.argv[x])
if url_count == 0:
  exit_unknown({'msg' : 'No URL specified'})

queue_stats_url = sidekiq_web_ui + '/stats/queues'

cmdline = argparse.ArgumentParser()
cmdline.add_argument('--sleep', default=sleeptime)

parsed_data = get_queue_sizes()

queues = {}
for q, v in parsed_data.items():
  queues[q] = {}
  queues[q]['size1'] = v
  noise_opt = "--{}-noise".format(q)
  crit_opt = "--{}-crit".format(q)
  warn_opt = "--{}-warn".format(q)
  cmdline.add_argument(noise_opt, default=noise)
  cmdline.add_argument(crit_opt, default=crit)
  cmdline.add_argument(warn_opt, default=warn)

args = cmdline.parse_args(my_args)
sleep = args.sleep

for arg in args.__dict__.items():
  if arg[0] == 'sleep':
    continue
  opt = arg[0].split('_')
  a = opt[0]
  b = opt[1]
  queues[a][b] = arg[1]

above_noise = 0
for q, v in queues.items():
  if v['size1'] > v['noise']:
    above_noise += 1
    # since we're already looping through let's assume the status is OK
    # we'll override it later if it's not
    v['status'] = STATE_OK
if above_noise == 0:
  exit_ok({'msg': 'All queues under threshold'})

time.sleep(sleep)

parsed_data = get_queue_sizes()

for q, v in parsed_data.items():
  queues[q]['size2'] = v
  queues[q]['rate'] = queues[q]['size1'] - queues[q]['size2']

status = ""
crit_count = 0
warn_count = 0
for q, v in queues.items():
  if v['rate'] <= v['crit']:
    crit_count += 1
    status += "'{}' is processing {} messages (or fewer) per {} seconds.\n".format(q, v['rate'], sleep)
    v['status'] = STATE_CRIT

for q, v in queues.items():
  if v['status'] == STATE_CRIT:
    continue
  if v['rate'] <= v['warn']:
    warn_count += 1
    status += "'{}' is processing {} messages (or fewer) per {} seconds.\n".format(q, v['rate'], sleep)

if crit_count > 0:
  exit_crit({'msg': status})
if warn_count > 0:
  exit_warn({'msg': status})
exit_ok({'msg': "All queues are processing acceptably fast"})
