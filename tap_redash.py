import logging
import pandas as pd
import requests as req
import json
import singer

logger = singer.get_logger()


class Redash:
    # add a check for when json is read in to make sure all RCKs are inc.
    REQUIRED_CONFIG_KEYS = ['QUERY_URL', 'LOGIN_URL', 'email', 'password', 'API_KEY', 'QUERY_ID']

    def __init__(self):
        try:
            # make it so this can be ran with any name as long as its after -c
            with open('config.json') as data:
                self.__config = json.load(data)
        except Exception as e:
            raise IOError("File was not found: ", e)
        self.auth()
        self.query_id = self.__config['QUERY_ID']
        self.__data = self.get_query_data(self.query_id)
        self.col_types, self.col_names = [], []

    def auth(self):
        self.__session = req.Session()
        # get redash login details
        auth_params = {
            "email": self.__config['email'],
            "password": self.__config['password']
        }
        request = self.__session.post(
            self.__config["LOGIN_URL"],
            data=auth_params
        )
        # return auth code for validation later
        return request.status_code

        # switch statement

    def switch(self, case_var):
        # logging.error(type(case_var))
        return {
            int: "number",
            float: "number",
            str: "string",
            dict: "dict"

        }.get(type(case_var), None)

    def get_query_data(self, query_id):

        # query url
        url = '{}/{}/results.json'.format(self.__config["QUERY_URL"], query_id)

        # get the response from the redash query in json
        data_json = None
        try:
            response = self.__session.get(url, params={'api_key': self.__config["API_KEY"]})
            # find query data in the json payload
            data_json = json.loads(response.text)
        except req.HTTPError:
            print("Error in getting query results!")
        if data_json is not None:
            # amend this code to print schema not a pd.df
            # load query data into data frame
            result = data_json['query_result']['data']['rows']
            # with open("query-data/results_{}.json".format(query_id), 'w') as f:
            #     f.write(json.dumps(result))
            # data = pd.DataFrame(data_json["query_result"]["data"]["rows"])
            # data.fillna('', inplace=True)
            return result
        else:
            return None

    def generate_schema(self):

        def get_properties():
            result = []
            for k, v in self.__data[0].items():
                # logging.error("{} - {}".format(k, v))
                obj = {k: {"type": ["null", self.switch(v)]}}
                result.append(obj)
            return result

        filename = "tap_redash/{}_properties".format(self.query_id)
        # create schema
        schema = {"stream": json.dumps(self.query_id),
                  "stream_id": json.dumps(self.query_id),
                  "schema": {"type ": ["null", "object"],
                             "additionalProperties": False,
                             "properties": get_properties()},
                  "key_properties": []
                  }
        # data = json.dumps(result)
        # with open(filename, 'w') as f:
        #     json.dump(schema, f, separators=(',', ': '), ensure_ascii=False)

        return self.__data, schema

    def output_to_stream(self, stream_name, schema):
        singer.write_schema(stream_name, schema, schema['key_properties'])
        singer.write_records(stream_name, self.generate_schema()[0])

    def do_discover(self):
        """
        Discovery mode will generate a properties file in a schema JSON format
        :param query_id:
        :return: JSON file
        """
        print(json.dumps(self.generate_schema()[1], indent=2))

    def get_stream(self, schema):
        selected_stream = []
        for stream in schema:
            selected_stream.append(stream['stream_id'])

        return selected_stream


def main():
    rdash = Redash()
    args = singer.utils.parse_args(rdash.REQUIRED_CONFIG_KEYS )
    if args.discover:
        rdash.do_discover()
    else:
        # schema is the passed arg or it needs to be generated if not
        schema = args.properties if args.properties else rdash.do_discover()
        rdash.output_to_stream(schema['stream_id'], schema)
        # data = rdash.generate_schema()[0]


if __name__ == "__main__":
    main()
