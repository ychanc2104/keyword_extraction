import argparse
import httplib2
import os
from googleapiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools


class GoogleOAuth2:
    def __init__(self, SCOPES=['https://www.googleapis.com/auth/webmasters.readonly'], CLIENT_SECRETS_PATH='client_secrets.json',
                 OAUTH2_CREDENTIALS_PATH='authorizedcreds.dat', api_name='searchconsole', version='v1'):
        super().__init__()
        self.SCOPES = SCOPES
        self.CLIENT_SECRETS_PATH = os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_PATH)
        self.OAUTH2_CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), OAUTH2_CREDENTIALS_PATH)
        self.service = self.connect_resource_server(api_name=api_name, version=version)

    # Take the credentials and authorize them using httplib2, api_name and version: https://googleapis.github.io/google-api-python-client/docs/dyn/
    def connect_resource_server(self, api_name='searchconsole', version='v1'):
        credentials = self._fetch_credentials()
        http = httplib2.Http()  # Creates an HTTP client object to make the http request
        http = credentials.authorize(
            http=http)  # Sign each request from the HTTP client with the OAuth 2.0 access token
        service = build(api_name, version,
                        http=http)  # Construct a Resource to interact with the API using the Authorized HTTP Client.
        return service

    # Prepare credentials and authorize HTTP
    # If they exist, get them from the storage object
    # credentials will get written back to the 'authorizedcreds.dat' file.
    def _fetch_credentials(self):
        OAUTH2_CREDENTIALS_PATH = self.OAUTH2_CREDENTIALS_PATH
        flow = self._create_flow()
        flags = self._create_parser()
        storage = file.Storage(OAUTH2_CREDENTIALS_PATH)
        credentials = storage.get()
        # If authenticated credentials don't exist, open Browser to authenticate
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)  # Add the valid creds to a variable
        return credentials

    # Create a parser to be able to open browser for Authorization
    def _create_parser(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         parents=[tools.argparser])
        flags = parser.parse_args([])
        return flags

    # Creates an authorization flow from a clientsecrets file.
    # Will raise InvalidClientSecretsError for unknown types of Flows.
    def _create_flow(self):
        SCOPES = self.SCOPES
        CLIENT_SECRETS_PATH = self.CLIENT_SECRETS_PATH
        flow = client.flow_from_clientsecrets(CLIENT_SECRETS_PATH, scope = SCOPES,
                                              message = tools.message_if_missing(CLIENT_SECRETS_PATH))
        return flow