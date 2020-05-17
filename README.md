AAAA provides two different APIs:
- The integration (piblic) API that provides access to AAAA data for any authorized application.
- The private API that should only be called by AAA applications.


API Entry Points
The entry point is a Swagger page that documents the API.

IMAP Integration API
	prod url
	test url
	

IMAP Private API
The private API is made available via the /api URL prefix and can be accessed on the appliance server directly:
	https://w?????:?/api
	
Or using BigIP address as following: 
	https://aaaa-test-a/api
		

		


Errors
All errors are returned with an appropriate HTTP status code. The most common response codes are shown below.

2xx: Success
	200 (OK)

4xx: Client Error
400 (Bad Request) Inndicates that the client's request contains incorrect syntax.

401 (Unauthorized) Indicates that the request requires authentication.

403 (Forbidden) Indicates that the user does not have the necessary permissions for the resource. 

404 (Not Found} Indicates that the server can’t map the client’s URI to a resource.

5xx: Server Error
500 (Internal Server Error)	 A generic error response when the server throws an exception.

Some requests return extended error information encoded as JSON object. For example:

{
  "code" : "BadRequest",
  "message" : "An invalid request has been made."
}


Configuration

The AAA API gets its configuration settings from within the <?????> element in the web.config file. For example:



Logging
The AAAA API uses NLog as its underlying logging provider. By default, the logging can be configured via NLog.config file.

For more details, see https://github.com/nlog/nlog/wiki/Configuration-file
