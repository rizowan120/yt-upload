import os
from google_auth_oauthlib.flow import Flow

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

flow = Flow.from_client_secrets_file(
    "client_secret.json", SCOPES, redirect_uri="http://localhost:8080/"
)
url, _ = flow.authorization_url(access_type="offline", prompt="consent")
print("\nOpen this URL in your browser:\n")
print(url)

redirected = input("\nAfter approving, paste the FULL address from the address bar here: ").strip()
flow.fetch_token(authorization_response=redirected)

with open("token.json", "w") as f:
    f.write(flow.credentials.to_json())
print("\nSaved token.json")
