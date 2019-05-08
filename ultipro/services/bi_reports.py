import time

from zeep import Client as ZeepClient
from ultipro.services import bi_data, bi_stream

def execute_and_fetch(client, report_path, delimiter=','):
    context = bi_data.log_on_with_token(client)
    k = bi_data.execute_report(client, context, report_path, delimiter=delimiter)
    n = 1
    while n <= 10:
        try:
            r = bi_stream.retrieve_report(client, k)
            resp = r['body']['ReportStream'].decode('unicode-escape')
        except AttributeError:
            print('tries: {}'.format(n))
            time.sleep(5)
            n += 1
        else:
            return resp
