from flask_restful import Resource
from flask_restful.reqparse import RequestParser
from flask import jsonify
import common.utils
import common.jsonutils

from common.decorators import processPreRequest

arguments = RequestParser()

class getRegions(Resource):

    @processPreRequest(quota_quantity=0)
    def get(self, **kwargs):

        _area_models = common.jsonutils.get_all_region_info()

        return jsonify({"regions": list(_area_models.keys())}) 