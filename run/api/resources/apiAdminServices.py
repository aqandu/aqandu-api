from flask_restful import Resource, reqparse
from common.params import URL_PARAMS
from common.api_admin_utils import FS_API_OBJ, FS_ACCESS
import common.utils
from flask import jsonify, make_response
import common.api_request_key_infos

passcode = common.utils.getConfigData()["PASSCODE"]

createAPIKeyArgs = reqparse.RequestParser()
createAPIKeyArgs.add_argument(URL_PARAMS.PASSCODE,   type=str, required=True)
createAPIKeyArgs.add_argument(URL_PARAMS.IDENTIFIER, type=str, required=True)
createAPIKeyArgs.add_argument(URL_PARAMS.QUOTA,      type=int, required=True)
createAPIKeyArgs.add_argument(URL_PARAMS.KEY,        type=str, required=False, default=None)
class createAPIObj(Resource):
    def get(self, **kwargs):
        args = createAPIKeyArgs.parse_args()
        
        if passcode != args[URL_PARAMS.PASSCODE]:
            return []
        
        api_obj = FS_API_OBJ(
            identifier=args[URL_PARAMS.IDENTIFIER],
            quota=args[URL_PARAMS.QUOTA],
            new=True,
            key=args[URL_PARAMS.KEY]
        )

        if FS_ACCESS.identifier_exists(args[URL_PARAMS.IDENTIFIER]):
            return jsonify({"message": f"FAILURE: Already exists for identifier: {args[URL_PARAMS.IDENTIFIER]}"})

        FS_ACCESS.create_api_obj(api_obj)

        return make_response("success", 200)
        

updateAPIObjArgs = reqparse.RequestParser()
updateAPIObjArgs.add_argument(URL_PARAMS.PASSCODE,   type=str, required=True)
updateAPIObjArgs.add_argument(URL_PARAMS.IDENTIFIER, type=str, required=True)
updateAPIObjArgs.add_argument(URL_PARAMS.QUOTA,      type=int, required=False, default=None)
updateAPIObjArgs.add_argument(URL_PARAMS.KEY,        type=str, required=False, default=None)
updateAPIObjArgs.add_argument(URL_PARAMS.QUOTA_USED, type=int, required=False, default=None)
class updateAPIObj(Resource):
    def get(self, **kwargs):
        args = updateAPIObjArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        update_obj = FS_API_OBJ(
            identifier=args[URL_PARAMS.IDENTIFIER],
            quota=args[URL_PARAMS.QUOTA],
            key=args[URL_PARAMS.KEY],
            used=args[URL_PARAMS.QUOTA_USED]
        )

        FS_ACCESS.update_api_obj(update_obj)

        return make_response("success", 200)


getQuotaArgs = reqparse.RequestParser()
getQuotaArgs.add_argument(URL_PARAMS.KEY, type=str, required=True)
class getQuota(Resource):
    def get(self, **kwargs):
        args = getQuotaArgs.parse_args()
        val = FS_ACCESS.get_quota_for_key(args[URL_PARAMS.KEY])
        return jsonify({"quota": val, "units": "days accessed by query"})


getQuotaUsedArgs = reqparse.RequestParser()
getQuotaUsedArgs.add_argument(URL_PARAMS.KEY, type=str, required=True)
class getQuotaUsed(Resource):
    def get(self, **kwargs):
        args = getQuotaUsedArgs.parse_args()
        val = FS_ACCESS.get_quota_used_for_key(args[URL_PARAMS.KEY])
        return jsonify({"quota used": val, "units": "days accessed by query"})


getQuotaLeftArgs = reqparse.RequestParser()
getQuotaLeftArgs.add_argument(URL_PARAMS.KEY, type=str, required=True)
class getQuotaRemaining(Resource):
    def get(self, **kwargs):
        args = getQuotaLeftArgs.parse_args()
        val = FS_ACCESS.get_quota_remaining_for_key(args[URL_PARAMS.KEY])
        return jsonify({"quota remaining": val, "units": "days accessed by query"})


getKeyArgs = reqparse.RequestParser()
getKeyArgs.add_argument(URL_PARAMS.PASSCODE,   type=str, required=True)
getKeyArgs.add_argument(URL_PARAMS.IDENTIFIER, type=str, required=True)
class getKey(Resource):
    def get(self, **kwargs):
        args = getKeyArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []
        
        api_obj = FS_ACCESS.get_api_obj_for_identifier(args[URL_PARAMS.IDENTIFIER])
        print(api_obj.to_dict())
        return jsonify({"key": api_obj.key})