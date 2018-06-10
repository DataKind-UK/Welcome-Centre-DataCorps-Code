import pickle

from api.resources.retrain import train_model_from_json
import os
import json

input_directory = os.environ.get('INPUT_DIR', '/upload')
output_directory = os.environ.get('OUTPUT_DIR', '/output')

files = os.listdir(input_directory)

if len(files) > 0:
    model_name = files[0]
    request = json.load(open(os.path.join(input_directory, model_name), 'r'))
    model, message = train_model_from_json(request)
    pickle.dump(model, open(os.path.join(output_directory, model_name), 'wb'))