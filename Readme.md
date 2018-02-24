# Daf Service Storage

The project exposes an api endpoint to query the dataset stored into the DAF.


The endpoint exposes an [openapi 3 endpoint](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md).
The project is build using [play framework 2.6.11](https://www.playframework.com/documentation/2.6.x/ScalaHome) and the openapi api are generated using [swagger-play](https://github.com/swagger-api/swagger-play).


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
