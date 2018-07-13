# tap-redash
Singer.IO tap to obtain data using the Redash API based on the query's ID


## Set up

Run terminal command:

`pip install -e git+https://github.com/domb16/tap-redash.git#egg=tap-redash`

Run `tap-redash` this should give you an error since it requires a config file the structure can be found in [config_sample.json](./config_sample.json)

### Discover mode

Run the tap in discover mode `tap-redash -c config.json -d > location/schema_query.json`

Read the schema in to get the data with it using: `tap-redash -c config.json -p location/schema_query.json`

Can be piped to any other target from the [Singer.io](https://singer.io)
