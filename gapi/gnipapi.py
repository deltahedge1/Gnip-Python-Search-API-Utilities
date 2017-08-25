from functools import reduce
import re
import datetime
try:
    import ujson as json
except ImportError:
    import json

import requests
from tweet_parser.tweet import Tweet

BASE_URL = "https://gnip-api.twitter.com/search/"
BASE_ENDPOINT = "{api}/accounts/{account_name}/{label}"

GNIP_RESP_CODES = {
    '200': 'OK: The request was successful. The JSON response will be similar to the following:',
    '400': 'Bad Request: Generally, this response occurs due to the presence of invalid JSON in the request, or where the request failed to send any JSON payload. ',
    '401': 'Unauthorized: HTTP authentication failed due to invalid credentials. Log in to console.gnip.com with your credentials to ensure you are using them correctly with your request. ',
    '404': 'Not Found: The resource was not found at the URL to which the request was sent, likely because an incorrect URL was used.',
    '422': 'Unprocessable Entity: This is returned due to invalid parameters in a query or when a query is too complex for us to process. – e.g. invalid PowerTrack rules or too many phrase operators, rendering a query too complex.',
    '429': 'Unknown Code: Your app has exceeded the limit on connection requests. The corresponding JSON message will look similar to the following:',
    '500': "Internal Server Error: There was an error on Gnip's side. Retry your request using an exponential backoff pattern.",
    '502': "Proxy Error: There was an error on Gnip's side. Retry your request using an exponential backoff pattern.",
    '503': "Service Unavailable: There was an error on Gnip's side. Retry your request using an exponential backoff pattern."
}


def gen_endpoint(search_api, account_name, label, count_endpoint=False):
    # helper for modifying count data
    label = label if not label.endswith(".json") else label.split(".")[0]
    endpoint = BASE_ENDPOINT.format(api=search_api,
                                    account_name=account_name,
                                    label=label)
    if count_endpoint:
        endpoint = endpoint + "/counts.json"
    else:
        endpoint = endpoint + ".json"

    endpoint = BASE_URL + endpoint
    return endpoint



def retry(func):
    """
    Decorator to handle API retries and exceptions.
    Args:
        func (function): function for decoration
    """
    def retried_func(*args, **kwargs):
        MAX_TRIES = 3
        tries = 0
        while True:
            try:
                resp = func(*args, **kwargs)

            except requests.exceptions.ConnectionError as exc:
                exc.msg = "Connection error for session; exiting"
                raise exc

            except requests.exceptions.HTTPError as exc:
                exc.msg = "HTTP error for session; exiting"
                raise exc

            if resp.status_code != 200 and tries < MAX_TRIES:
                print("retrying request; current status code: {}"
                      .format(resp.status_code))
                tries += 1
                continue

            break

        if resp.status_code != 200:
            print("HTTP Error code: {}: {}"
                  .format(resp.status_code,
                          GNIP_RESP_CODES[str(resp.status_code)]))
            print("rule payload: {}".format(kwargs["rule_payload"]))
            raise requests.exceptions.HTTPError

        return resp

    return retried_func


def convert_utc_time(datetime_str):
    """Handles datetime argument conversion to the GNIP API format, which is
    `YYYYMMDDHHSS`. Flexible passing of date formats.

    Args:
        datetime_str (str): the datestring, which can either be in GNIP API
        Format, ISO date format (YYYY-MM-DD), or ISO datetime format (YYYY-MM-DD HH:mm)
    Returns:
        string of GNIP API formatted date.

    Example:
        >>> convert_utc_time("201708020000")
        '201708020000'
        >>> convert_utc_time("2017-08-02")
        '201708020000'
        >>> convert_utc_time("2017-08-02 00:00")
        '201708020000'
    """

    if not datetime_str:
        return None
    if not set(['-', ':']) & set(datetime_str):
        _date = datetime.datetime.strptime(datetime_str, "%Y%m%d%H%M")
    else:
        try:
            _date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
        except ValueError:
            _date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d")

    return _date.strftime("%Y%m%d%H%M")


def gen_rule_payload(pt_rule, max_results=100,
                     from_date=None, to_date=None, count_bucket=None,
                     stringify=True):

    """Generates the dict or json payload for a PowerTrack rule.

    Args:
        pt_rule (str): the string veriosn of a powertrack rule, e.g., "kanye
            west has:geo". Accepts multi-line strings for ease of entry.
        max_results (int): max results for the batch.
        from_date (str or None): date format as specified by
            `convert_utc_time` for the starting time of your search.

        to_date (str or None): date format as specified by
            `convert_utc_time` for the end time of your search.

        count_bucket (str or None): if using the counts api endpoint, will
            define the count bucket for which tweets are aggregated.
        stringify (bool): specifies the return type, `dict` or json-formatted
            `str`.

    Example:

        >>> gen_rule_payload("kanye west has:geo",
            ...              from_date="2017-08-21",
            ...              to_date="2017-08-22")
        '{"query":"kanye west has:geo","maxResults":100,"toDate":"201708220000","fromDate":"201708210000"}'
    """

    pt_rule = ' '.join(pt_rule.split()) # allows multi-line strings
    payload = {"query": pt_rule,
               "maxResults": max_results,
              }
    if from_date:
        payload["toDate"] = convert_utc_time(to_date)
    if to_date:
        payload["fromDate"] = convert_utc_time(from_date)

    if count_bucket:
        if set(["day", "hour", "minute"]) & set([count_bucket]):
            payload["bucket"] = count_bucket
            del payload["maxResults"]
        else:
            print("invalid count bucket: provided {}".format(count_bucket))
            raise ValueError

    return json.dumps(payload) if stringify else payload


def make_session(username, password):
    """Creates a Requests Session for use.

    Args:
        username (str): username for the session
        password (str): password for the user
    """

    session = requests.Session()
    session.headers = {'Accept-encoding': 'gzip'}
    session.auth = username, password
    return session


def merge_dicts(*dicts):
    """
    Helpful function to merge / combine dictionaries and return a new
    dictionary.
    """
    def _merge_dicts(dict1, dict2):
        return {**dict1, **dict2}

    return reduce(_merge_dicts, dicts)


@retry
def request(session, url, rule_payload, **kwargs):
    """
    Executes a request with the given payload and arguments.

    Args:
        session (requests.Session): the valid session object
        url (str): Valid API endpoint
        rule_payload (str or dict): rule package for the POST. if you pass a
            dictionary, it will be converted into JSON.

    """
    if isinstance(rule_payload, dict):
        rule_payload = json.dumps(rule_payload)
    result = session.post(url, data=rule_payload)
    return result


class ResultStream:
    """Class to represent an API query that handles two major functionality
    pieces: wrapping metadata around a specific API call and automatic
    pagination of results.
    """

    def __init__(self, username, password, url, rule_payload,
                 max_results=1000, tweetify=True):
        """
        Args:
            username (str): username
            password (str): password
            url (str): API endpoint; should be generated using the
                `gen_endpoint` function.
            rule_payload (json or dict): payload for the post request
            max_results (int): max results that will be fetched from the API.
            tweetify (bool): If you are grabbing tweets and not counts, use the
                tweet parser library to convert each raw tweet package to a Tweet
                with lazy properties.

        """

        self.username = username
        self.password = password
        self.url = url
        if isinstance(rule_payload, str):
            rule_payload = json.loads(rule_payload)
        self.rule_payload = rule_payload
        self.tweetify = tweetify
        self.max_results = max_results

        self.total_results = 0
        self.n_pages = 0
        self.session = None
        self.current_tweets = None
        self.next_token = None
        self._tweet_func = Tweet if tweetify else lambda x: x


    def __iter__(self):
        """
        Handles pagination of results. Uses new yield from syntax.
        """
        tweets = (self._tweet_func(t) for t in self.current_tweets)
        for i, tweet in enumerate(tweets):
            if self.total_results >= self.max_results:
                break
            yield tweet
            self.total_results += 1

        if self.total_results >= self.max_results:
            print("finished grabbing {} tweets".format(self.total_results))
            return

        if self.next_token:
            self.rule_payload = merge_dicts(self.rule_payload, ({"next": self.next_token}))
            self.n_pages += 1
            print("total n_pages read in request so far: {}".format(self.n_pages))
            self.execute_request()
            yield from iter(self)

    def init_session(self):
        self.session = make_session(self.username, self.password)

    def check_counts(self):
        if "counts" in re.split("[/.]", self.url):
            print("disabling tweet parsing due to counts api usage")
            self._tweet_func = lambda x: x

    def end_stream(self):
        self.current_tweets = None
        self.session.close()

    def start_stream(self):
        self.init_session()
        self.check_counts()
        self.execute_request()
        return iter(self)

    def execute_request(self):
        resp = request(session=self.session,
                       url=self.url,
                       rule_payload=self.rule_payload)
        resp = json.loads(resp.content.decode(resp.encoding))
        self.next_token = resp.get("next", None)
        self.current_tweets = resp["results"]

    def __repr__(self):
        str_ = "\n\t".join(["ResultStream Params:",
                            self.username,
                            self.url,
                            str(self.rule_payload),
                            str(self.tweetify),
                            str(self.max_results)])
        return str_
