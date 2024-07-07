import json
import os

path_to_json = input('Enter the path to JSONs: ').replace("\\","/")
json_files = [json_files for json_files in os.listdir(path_to_json) if json_files.endswith('.json')]

for file in json_files:
    print('Updating File: ', os.path.join(path_to_json, file))

    # Read the file
    with open(os.path.join(path_to_json, file), 'r') as f:
        data = json.load(f)

    # attribute to update
    #data["sourceDatabaseName"] = 'Global_DRB'
    #data["sourceSchemaName"] = 'Global_DRB.dbo'
    data["scheduleGroup"] = "DAILY-FULL-LOAD"
    data["systemName"] = "qtrac"


    # Write the update JSON
    with open(os.path.join(path_to_json, file), 'w') as f:
        json.dump(data, f, indent=4)
     #   json.dump(" ",f)
     #   json.dump(data1, f, indent=4)
print('Completed updating JSONs')