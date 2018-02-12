import pandas as pd
import json


def parse_json_reponse_into_df_dict(json_string):
    j = json.loads(json_string)
    df_dict = {}
    for k, v in j.items():
        df_dict[k] = pd.DataFrame(v)
    return df_dict


def test_response_parser_on_file(path):
    with open(path, 'r') as f:
        json_string = f.read()
        df_dict = parse_json_reponse_into_df_dict(json_string)
    return df_dict

