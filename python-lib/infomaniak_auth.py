import requests


class InfomaniakAuth(requests.auth.AuthBase):
    def __init__(self, api_token=None):
        self.api_token = api_token

    def __call__(self, request):
        request.headers["Authorization"] = "Bearer {}".format(
            self.api_token
        )
        return request
