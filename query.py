from pyjsonq import JsonQ
from os import listdir
from os.path import join
import json
from sys import argv, exit

# searchable parent keys
# ['title', 'company', 'start_date', 'end_date', 'linkedin_company_url', 'skills', 'certifications', 'languages', 'name', 'location', 'area', 'industry', 'picture', 'organizations', 'groups', 'education', 'websites', 'profile_url', 'summary', 'current', 'timestamp', 'related_people', 'degree', 'pic_path', 'score']


# load the data from the ICWATCH-Data data directory (ICWATCH-Data/data)
# change 'my_path' to your local dir

# inputs:
# --help: prints out list of commands
# --dir: home directory location for ICWATCH-Data folder (i.e., /home/jynurso/Documents/Code)
# --key: key you want to search (i.e., 'name')
# --string: the search string
# --match: the match condition (see pyjsonq's readme for details)
# --searchkeys: returns list of searchable parent keys in ICWATCH files

# initialize search elements
args = argv
key = ''
key2 = '' # optional only for nested searches
match = ''
string = ''
case = True # set it as case insentive by default
my_path = '/home/jynurso/Documents/Code'
original_path = my_path + '/ICWATCH-Data/data/original_run'
second_path = my_path + '/ICWATCH-Data/data/second_set'

# check if arguments were passed from the user
if len( args) == 1:
  print("use --help to get list of commands")
  exit()

# arguments were passed so now handle them
if '--help' in args:
  # print help
  help_str = "--help: prints out list of commands\n--dir: home directory location for ICWATCH-Data folder (i.e., /home/jynurso/Documents/Code)\n--key: key you want ot search (i.e., name)\n--string: the search string\n--match: the match condition (see pyjsonq's readme for details)\n--searchkeys: returns list of searchable parent keys in ICWATCH files"
  print(help_str)
  exit()
if '--searchkeys' in args:
  # return the list of searchable keys
  keys_str = "['title', 'company', 'start_date', 'end_date', 'linkedin_company_url', 'skills', 'certifications', 'languages', 'name', 'location', 'area', 'industry', 'picture', 'organizations', 'groups', 'education', 'websites', 'profile_url', 'summary', 'current', 'timestamp', 'related_people', 'degree', 'pic_path', 'score']"
  print(keys_str)
  exit()

if '--dir' in args:
  # update my_path
  idx = args.index('--dir')
  my_path = args[idx+1] 
if '--key' in args:
  # save the search key
  idx = args.index('--key')
  key = args[idx+1]
if '--string' in args:
  # save the search string
  idx = args.index('--string')
  string = args[idx+1]
if '--match' in args:
  # save the match condition
  idx = args.index('--match')
  match = args[idx+1]
if '--key2' in args:
  # save the second key
  idx = args.index('--key2')
  key2 = args[idx+1]
if '--case' in args:
  # save case sensitivity
  idx = args.index('--case')
  case = bool(args[idx+1])
if not key or not string or not match:
  print("Missing search key, string, and/or match, exiting.")
  exit()
else:
  print("There's thousands of files, they will take time. Searching...")
  # print(type(key))
  # print(match)
  # print(string)
  # exit()

# collect the name of json files only so we can load them into the querying package
original_files = [f for f in listdir(original_path) if f.endswith('.json')]
second_files = [f for f in listdir(second_path) if f.endswith('.json')]

# initialize results holders
results = []
count = 0
results_s = []
count_s = 0

# loop through all files in the original_files directory and save the results
for f in original_files: 
  #print(join(original_path,f))
  f_path = join(original_path,f)
  # data = open(f_path)
  # json_data = json.load(data)
  # if json_data:
  #   print(json_data[0].keys())
  qe = JsonQ(join(original_path,f))

  # if the key is plural, then we need to use at
  #if key2:
    #qe.at('title').count()
    #print(key)
    #print(key2)
    #res = qe.where(key, '=', key2)
    # check if there are nested keys
    #if len(res) > 0:
    #  res = qe.where(key, match, string).get()
    #else:
    #  return
    # print(type(res))
    # exit()
    # res = qe.at(key).where(key2, match, string).get()
  #else:
  res = qe.where(key, match, string).get()
  
  if res:
    count = count + 1
  results.append(res)

non_empty_results = [x for x in results if x]

with open(join(my_path, 'results.json'), 'w') as file:
  json.dump(non_empty_results, file)


# loop through all files in the econd_files directory and save the results
for f in second_files: 
  qe = JsonQ(join(second_path,f))
  res = qe.where(key, match, string).get()
  if res:
    count_s = count_s + 1
  results_s.append(res)

non_empty_results_s = [x for x in results_s if x]

with open(join(my_path, 'results_second.json'), 'w') as file:
  json.dump(non_empty_results_s, file)

print(count)
print(count_s)
