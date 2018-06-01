import json
import os

def fix_json_file(input_path, output_path):
    """This function fills in the Client Id in the referrals for Andrews retrain example file"""
    with open(input_path, 'r') as f:
        j = json.load(f)
    for item in j:
        client_id = item['client'][0]['clientid']
        referrals = item['referral']
        for ref in referrals:
            ref['clientid'] = client_id
    with open(output_path, 'w') as f1:
        json.dump(j, f1)

if __name__ == '__main__':
    fix_json_file('retrain_request.json', 'fixed_retrain_request.json')
