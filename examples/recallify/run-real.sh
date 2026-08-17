#!/usr/bin/env sh
set -eu

GOFLOW_URL=${GOFLOW_URL:-http://localhost:8081}
RECALLIFY_URL=${RECALLIFY_URL:-http://localhost:8080}
NOTE_FILE=${NOTE_FILE:-"$(dirname "$0")/go-notes.txt"}
TITLE=${TITLE:-Go Notes}
LEVEL=${LEVEL:-medium}
MCQ_COUNT=${MCQ_COUNT:-1}
EXTERNAL_REQUEST_ID=${EXTERNAL_REQUEST_ID:-"real-recallify-test-$(date +%s)"}
CALLBACK_URL=${CALLBACK_URL:-}
RECALLIFY_TOKEN=${RECALLIFY_TOKEN:-}
RECALLIFY_EMAIL=${RECALLIFY_EMAIL:-}
RECALLIFY_PASSWORD=${RECALLIFY_PASSWORD:-}

export RECALLIFY_EMAIL RECALLIFY_PASSWORD

if [ -z "$RECALLIFY_TOKEN" ] && [ -n "$RECALLIFY_EMAIL" ] && [ -n "$RECALLIFY_PASSWORD" ]; then
	echo "Logging in to Recallify at $RECALLIFY_URL"
	login_body=$(
		python3 - <<'PY'
import json
import os

print(json.dumps({
    "email": os.environ["RECALLIFY_EMAIL"],
    "password": os.environ["RECALLIFY_PASSWORD"],
}))
PY
	)
	login_response=$(
		curl -sS -f -X POST "$RECALLIFY_URL/api/user/login" \
			-H "Content-Type: application/json" \
			-d "$login_body"
	)
	RECALLIFY_TOKEN=$(
		LOGIN_RESPONSE=$login_response python3 - <<'PY'
import json
import os

print(json.loads(os.environ["LOGIN_RESPONSE"])["accessToken"])
PY
	)
	echo "Got Recallify access token"
fi

export NOTE_FILE TITLE LEVEL MCQ_COUNT EXTERNAL_REQUEST_ID CALLBACK_URL RECALLIFY_URL RECALLIFY_TOKEN

payload=$(
	python3 - <<'PY'
import json
import os

with open(os.environ["NOTE_FILE"], encoding="utf-8") as f:
    document_text = f.read()

body = {
    "document_text": document_text,
    "title": os.environ["TITLE"],
    "level": os.environ["LEVEL"],
    "mcq_count": int(os.environ["MCQ_COUNT"]),
    "external_request_id": os.environ["EXTERNAL_REQUEST_ID"],
    "recallify_url": os.environ["RECALLIFY_URL"],
}

if os.environ["CALLBACK_URL"]:
    body["callback_url"] = os.environ["CALLBACK_URL"]

if os.environ["RECALLIFY_TOKEN"]:
    body["recallify_bearer_token"] = os.environ["RECALLIFY_TOKEN"]

print(json.dumps(body))
PY
)

curl -sS -X POST "$GOFLOW_URL/demos/recallify/runs" \
	-H "Content-Type: application/json" \
	-H "Idempotency-Key: $EXTERNAL_REQUEST_ID" \
	-d "$payload" \
	-w "\nHTTP %{http_code}\n"
