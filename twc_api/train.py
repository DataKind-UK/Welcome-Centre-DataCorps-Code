from api import ParseJSONToTablesTransformer
from twc_api.api.model.train import train_model_from_json
import json
import logging
import pickle

logger = logging.getLogger('twc_logger')

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


if __name__ == '__main__':
    _, _, _, model = train_model_from_json(json.load(open('api/JSONExport20180803200350.JSON', 'r')),
                                           hyperparams={'n_estimators': 120})
    pickle.dump(model, open('twc_model_0.p', 'wb'))
    # model = pickle.load(open('tmp_model.p', 'rb'))
    # request = json.load(open('api/request1872.json', 'r'))
    # transformer = ParseJSONToTablesTransformer()
    # tables = transformer.transform(request)
    # print(model.predict(tables))
