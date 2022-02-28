from flask import Flask 
from flask_cors import CORS
from flask_restful import Api

from resources.getSensorData import getSensorData
from resources.getTimeAggregatedData import getTimeAggregatedData
from resources.getEstimateMap import getEstimateMap
from resources.getLiveSensors import getLiveSensors
from resources.getCorrectionFactors import getCorrectionFactors
from resources.getLocalSensorData import getLocalSensorData
from resources.getEstimateAtLocation import getEstimateAtLocation
from resources.getEstimateAtLocations import getEstimateAtLocations
from resources.getBoundingBox import getBoundingBox
from resources.getRegions import getRegions
from resources.nickname import nickname

from resources.apiAdminServices import createAPIObj
from resources.apiAdminServices import updateAPIObj
from resources.apiAdminServices import getKey
from resources.apiAdminServices import getQuota
from resources.apiAdminServices import getQuotaUsed
from resources.apiAdminServices import getQuotaRemaining

from resources.Documentation import Documentation

app = Flask(__name__)
api = Api(app)
app.config['CORS_HEADERS'] = "Content-Type"
CORS(app)

api.add_resource(getSensorData,          '/getSensorData')
api.add_resource(getTimeAggregatedData,  '/getTimeAggregatedData')
api.add_resource(getEstimateMap,         '/getEstimateMap')
api.add_resource(getLiveSensors,         '/getLiveSensors')
api.add_resource(getCorrectionFactors,   '/getCorrectionFactors')
api.add_resource(getLocalSensorData,     '/getLocalSensorData')
api.add_resource(getEstimateAtLocation,  '/getEstimateAtLocation')
api.add_resource(getEstimateAtLocations, '/getEstimateAtLocations')
api.add_resource(getBoundingBox,         '/getBoundingBox')
api.add_resource(getRegions,             '/getRegions')

api.add_resource(createAPIObj,      '/limited/createAPIObj')
api.add_resource(updateAPIObj,      '/limited/updateAPIObj')
api.add_resource(getKey,            '/limited/getKey')
api.add_resource(getQuota,          '/limited/getQuota')
api.add_resource(getQuotaUsed,      '/limited/getQuotaUsed')
api.add_resource(getQuotaRemaining, '/limited/getQuotaRemaining')
api.add_resource(nickname,          '/limited/nickname')

api.add_resource(Documentation, '/docs')

if __name__ == '__main__':
    import os
    os.environ['FLASK_ENV'] = 'development'
    app.run(host='127.0.0.1', port=8080, debug=True)
