# Daf Service Storage

[![Build Status](https://travis-ci.org/teamdigitale/daf-srv-storage.svg?branch=master)](https://travis-ci.org/teamdigitale/daf-srv-storage)

This project expose the api to access the dataset stored in the Data and Analytics Framework.
It is done using [play framework 2.6.13](https://www.playframework.com/documentation/2.6.x/ScalaHome) and the openapi api are generated using [swagger-play](https://github.com/swagger-api/swagger-play).

It exposes the following endpoints:

- GET /datasets/:uri -> extracts the first 1000 row from the dataset in format json
- GET /dataset/:uri/shema -> returns the schema of the dataset identified by the uri
- POST /dataset/:uri/search -> exposes an endpoint to query the dataset based on the following query object passed via post
  ```
  {
    "select" : ["columns", "or *", "or empty"],
    "where" : [
      {
        "property": "name of the column",
        "symbol": "" // it can be >, <, =, !=
        "value": "a valid value"
      }
    ],
    "groupBy": {
      "groupColumn": "",
      "conditions": [
        {
          "column": "name of the column",
          "aggregationFunction": "name of a valid sql aggregation function"
        }
      ]
    }
  }
  ```

Moreover, are exposed the routes:

- GET /datasets/:uri/:storageType                 

- GET /datasets/:uri/:storageType/schema

- POST /datasets/:uri/:storageType/search  

where `storageType` accepts a valid storage type among `hdfs`, `kudu`, `opentsdb`


## Run

To start the service run the following command on the shell

```bash

$ sbt run

```

The you can navigate to
 - `http://localhost:9000` to get the root of the endpoint, or
 - `http://localhost:9000/docs/` to see the swagger-ui for the service.

## Test

Al the test are place in the [test](./test) folder.
Type `sbt test` to run them.

## Notes

- The swagger-ui is configured by using [this link](https://www.cakesolutions.net/teamblogs/swagger-with-play-all-you-need-to-know)
