from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, multi_area
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask import jsonify, make_response
import common.utils
import common.jsonutils

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.AREA_MODEL, type=multi_area, help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=True)

class getBoundingBox(Resource):

    @processPreRequest(quota_quantity=0)
    def get(self, **kwargs):
        args = arguments.parse_args()
        areas = args[URL_PARAMS.AREA_MODEL]

        _area_models = common.jsonutils.get_all_region_info()

        bboxes = {}
        for area in areas:
            try:
                info = _area_models[area]
                bboxes[area] = info['bbox']
            except Exception as e:
                return make_response(jsonify(error=str(e)), 400)

        return jsonify(bboxes)