from math import ceil
import pytz
import numpy as np
from functools import wraps
from flask import request
from flask_restful import reqparse
from flask_restful.inputs import datetime_from_iso8601
from datetime import timezone, timedelta
import datetime
from urllib.parse import urlparse
import datetime
import pandas as pd
from common.db_utils import RequestAPI_DB_ACCESS
import common.api_admin_utils
from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, list_param, multi_area, bool_flag
import common.utils

MAX_QUERY_SIZE_DEVICE_DAYS = 100000

checkKeyDecorator = reqparse.RequestParser()
checkKeyDecorator.add_argument(URL_PARAMS.KEY, type=str, required=False, default=None)

#this is so the serviceName is homogenized
def processServiceName(requestPath):
    serviceName = request.path.lower()
    if '/' in serviceName:
        serviceName = serviceName.split('/')[-1]
    return serviceName

def getRemainingTimeToWait(requestKey, serviceName, host, hoursToRecharge):
    now = datetime.datetime.now(timezone.utc)
    time = now - timedelta(hours=hoursToRecharge, minutes=0)

    oldestRequestTime = common.db_utils.RequestAPI_DB_ACCESS.getOldestAPIRequestsSinceTime(serviceName, host, requestKey, time)
    td = now - oldestRequestTime

    secondsToRecharge = hoursToRecharge * 3600
    secondsDiff = (secondsToRecharge - td.seconds) # if we are calculating this, td.seconds will be larger than secondsToRecharge (the whole reason we're calling this function because they've made too many requests within their time limit)
    seconds = secondsDiff % 60 # calculate only seconds unit (until recharge)
    minutes = int(secondsDiff / 60) % 60 # calculate only minutes unit (until recharge)
    hours = int(secondsDiff / 3600) # calculate only hours unit (until recharge)
    return {'hours': hours, 'minutes': minutes, 'seconds': seconds, "secondsDiff": secondsDiff}


# https://stackoverflow.com/questions/627501/how-can-i-use-named-arguments-in-a-decorator
def processPreRequest(*dargs, **dkwargs):
    """
    This is a decorator that can accept arguments. You must define it as a function, like:
        @processPreRequest()
    Not like:
        @processPreRequest
    Or you'll get an error. The outermost function is just a function that returns
    a decorator internally.
    """
    def outer(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            url_args = checkKeyDecorator.parse_args()
            apiKey = url_args[URL_PARAMS.KEY]
            serviceName = processServiceName(request.path)
            host = urlparse(request.base_url).hostname
            
            # Skip localhost completely (I don't want to store tests, and this is faster)
            if host == 'localhost' or host == '127.0.0.1':
                RequestAPI_DB_ACCESS.recordServiceRequest(serviceName, host, apiKey)
                return func(*args, **kwargs)

            if apiKey is None:
                return {"message": "An API Key parameter [key=XXX] must be included in the URL to access this service."}

            #
            # Check Hourly Limit
            #
            # requestLimit = requestKeyInfo[serviceName]["Limit"]
            
            # if not requestKeyInfo: # no such request key exists
            #     return {"error_message": "Invalid Key"}
            # if serviceName not in requestKeyInfo or requestLimit == 0: # no such request key exists
            #     return {"error_message": "You do not have access for %s" % serviceName}

            # if requestLimit > 0: # -1 is unlimited so if it is -1 we don't want to ask the DB anything (it's a waste of time)
            #     time = datetime.datetime.now(timezone.utc) - timedelta(hours=requestKeyInfo[serviceName]["HoursToRecharge"], minutes=0)
            #     numRecentRequests = RequestAPI_DB_ACCESS.getNumberOfAPIRequestsSinceTime(serviceName, host, requestKey, time)
            #     if numRecentRequests > requestLimit: # number of requests exceeded key Limit within HoursToRecharge time
            #         remainingTimeToWait = getRemainingTimeToWait(requestKey, serviceName, host, requestKeyInfo[serviceName]["HoursToRecharge"])
            #         return {"error_message": "Request of %i requests per %i hour(s) Limit Exceeded for this API route. Please wait %i hour(s), %i minute(s) and %i second(s) before trying again" % (requestLimit, requestKeyInfo[serviceName]["HoursToRecharge"], remainingTimeToWait['hours'], remainingTimeToWait['minutes'], remainingTimeToWait['seconds'])}
            
            #
            # Check Size Limit
            #
            # device_days = estimateQuerySizeInDateRange()
            # if device_days > MAX_QUERY_SIZE_DEVICE_DAYS:
            #     return {"error_message": f"Data request too large. Maximum is {MAX_QUERY_SIZE_DEVICE_DAYS} device-days (This request would have been {device_days})."}

            #
            # Check quota (query-days)
            #
            if 'quota_quantity' not in dkwargs:
                quota_units_this_query = getDaysInQuery()
            else:
                quota_units_this_query = int(dkwargs['quota_quantity'])
            
            if quota_units_this_query != -1:    # -1 indicates they can run the query even if they're out of quota
                api_obj = common.api_admin_utils.FS_ACCESS.get_api_obj_for_key(apiKey)
                if api_obj.quota != -1 and api_obj.used + quota_units_this_query > api_obj.quota:
                    return {"error_message": f"Insufficient quota. Your quota: {api_obj.quota}. Remaining: {api_obj.remaining}. This query will cost: {quota_units_this_query}. Quotas reset at 12:00 AM UTC on the 1st of each month."}

            #
            # Passed, record the request
            #
            RequestAPI_DB_ACCESS.recordServiceRequest(serviceName, host, apiKey, url=request.url)

            #
            # Execute the function and record the result
            #
            data = func(*args, **kwargs)
            
            #
            # Update the quota after executing the route
            #
            if quota_units_this_query != -1:
                if quota_units_this_query and data.status_code == 200:
                    api_obj.update_quota_values(quota_units_this_query)
                    common.api_admin_utils.FS_ACCESS.update_api_obj(api_obj)
            
            return data
        return wrapper
    return outer

queryDaysArgs = reqparse.RequestParser()
queryDaysArgs.add_argument(URL_PARAMS.START_TIME, type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME, required=False, default=None)
queryDaysArgs.add_argument(URL_PARAMS.END_TIME,   type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,   required=False, default=None)
def getDaysInQuery():
    args = queryDaysArgs.parse_args()
    start_datetime = args[URL_PARAMS.START_TIME]
    end_datetime = args[URL_PARAMS.END_TIME]

    if start_datetime and end_datetime:
        days = 1 + (end_datetime - start_datetime).days
    else:
        days = 1

    return days



sizeEstimateArgs = reqparse.RequestParser()
sizeEstimateArgs.add_argument(URL_PARAMS.AREA_MODEL,    type=multi_area,            help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=False, default=multi_area("all"))
sizeEstimateArgs.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,         required=True)
sizeEstimateArgs.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,           required=True)
sizeEstimateArgs.add_argument(URL_PARAMS.SENSOR_SOURCE, type=str,                   help=PARAMS_HELP_MESSAGES.SENSOR_SOURCE,      required=False, default="all")
def estimateQuerySizeInDateRange():
    args = sizeEstimateArgs.parse_args()

    start_datetime = args[URL_PARAMS.START_TIME].astimezone(pytz.utc)
    end_datetime = args[URL_PARAMS.END_TIME].astimezone(pytz.utc)

    #
    # Form the query
    #
    
    # If the start date is this month/year, we need to use previous month because telemetry.statistics only updates previous month
    start_date = start_datetime.date()
    now = datetime.datetime.utcnow()
    if (now.year, now.month) == (start_date.year, start_date.month):   
        if start_date.month == 1: # Need to go to previous year
            start_date = start_date.replace(year=start_date.year - 1, month=12)
        else:
            start_date = start_date.replace(month=start_date.month - 1)
    else:
        start_date = args[URL_PARAMS.START_TIME].date()
    
    # Set it to the first day of the month
    start_date = start_date.replace(day=1)
    end_date = end_datetime.date()

    bq_client = common.utils.getBigQueryClient()
    regions_clause = f'''({' OR '.join([f'Region="{region}"' for region in args[URL_PARAMS.AREA_MODEL]])})'''

    if args[URL_PARAMS.SENSOR_SOURCE] == 'all':
        sources_clause = 'True'
    else:
        sources_clause = f'Source="{args[URL_PARAMS.SENSOR_SOURCE]}"'
    query = f"""
SELECT
    Date,
    SUM(NumDevices) AS NumDevices
FROM
    `telemetry.statistics`
WHERE
    Date >= "{start_date}"
    AND
    Date < "{end_date}"
    AND
    {regions_clause}
    AND
    {sources_clause}
GROUP BY
    Date
ORDER BY
    Date
    """

    job = bq_client.query(query)
    df = pd.DataFrame([dict(r) for r in job.result()]).drop_duplicates()
    
    #
    # Calculate the number of hours in each month of the query
    #

    # date range from start to end of query with stops at beginning of each month traversed
    # freq='MS' means put a point at the start of each month between start and end
    date_range = list(pd.date_range(start=start_datetime, end=end_datetime, freq='MS'))
    if len(date_range) == 0 or date_range[0] != start_datetime:
        date_range = [start_datetime] + date_range
    if date_range[-1] != end_datetime:
        date_range = date_range + [end_datetime]
    date_range = pd.Series(date_range)

    # Calculate the number of days in each month of the query
    # 1 Day = 86400 seconds
    df['NumDays'] = date_range.diff().apply(lambda x: x.total_seconds() / 86400).dropna().values

    #
    # Calculate the number of device-days of data: sum(# devs X # hours in month)
    #
    device_days = (df['NumDevices'] * df['NumDays']).sum()
    device_days = int(ceil(device_days))

    print(f'\nRequesting {device_days} device-days of data\n')
    
    return device_days