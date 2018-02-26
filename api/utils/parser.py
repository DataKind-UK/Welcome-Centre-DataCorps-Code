import pandas as pd
import json


def parse_json_reponse_into_df_dict(json_data):
    df_dict = {}
    for k, v in json_data.items():
        df_dict[k] = pd.DataFrame(v)
    return df_dict


def test_response_parser_on_file(path):
    with open(path, 'r') as f:
        json_string = f.read()
        json_data = json.loads(json_string)
        df_dict = parse_json_reponse_into_df_dict(json_data)
    return df_dict

