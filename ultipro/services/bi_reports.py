import backoff

from ultipro.helpers import backoff_hdlr
from ultipro.services import bi_data, bi_stream

@backoff.on_exception(backoff.expo, AttributeError, max_tries=10, on_backoff=backoff_hdlr)
def execute_and_fetch(client, report_path, delimiter=','):
    print("execute and fetch")
    context = bi_data.log_on_with_token(client)
    k = bi_data.execute_report(client, context, report_path, delimiter=delimiter)
    r = bi_stream.retrieve_report(client, k)
    return r['body']['ReportStream'].decode('unicode-escape')
