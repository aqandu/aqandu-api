from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, nickname_check
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask import make_response
import common.utils
import common.jsonutils

from common.decorators import processPreRequest

"""
"""

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.DEVICE, type=str, help=PARAMS_HELP_MESSAGES.DEVICE, required=True)
arguments.add_argument(URL_PARAMS.NICKNAME, type=nickname_check, help=PARAMS_HELP_MESSAGES.NICKNAME, default=True)

class nickname(Resource):

    @processPreRequest(quota_quantity=-1)
    def get(self, **kwargs):
        args = arguments.parse_args()

        device = args[URL_PARAMS.DEVICE]
        nickname = args[URL_PARAMS.NICKNAME]

        # Perform the UPDATE query
        query = f'''
        UPDATE
            `meta.devices`
        SET
            Nickname = "{nickname}"
        WHERE
            DeviceID = "{device}"
        '''

        common.utils.getBigQueryClient().query(query)

        return make_response('success', 200)