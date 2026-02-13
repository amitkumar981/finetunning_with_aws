import json, os, time, boto3

runtime = boto3.client("sagemaker-runtime")
dynamo = boto3.resource("dynamodb").Table(os.environ["LOG_TABLE"])
ENDPOINT = os.environ["SAGEMAKER_ENDPOINT"]

def safe_json(value):
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)

def try_parse_json(s: str):
    try:
        return json.loads(s)
    except Exception:
        return {"_raw": s}

def _parse_body(event):
    """
    Support:
      - API Gateway proxy: {"body": "{ \"inputs\": \"...\" }"}
      - API Gateway proxy with base64: {"isBase64Encoded": true, "body": "..."}
      - Direct invoke: {"inputs": "..."}
    """
    if not isinstance(event, dict):
        return {}

    if "body" in event:
        raw = event["body"]

        if event.get("isBase64Encoded") is True and isinstance(raw, str):
            import base64
            try:
                raw = base64.b64decode(raw).decode("utf-8", errors="replace")
            except Exception:
                return {}

        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {}
        if isinstance(raw, dict):
            return raw
        return {}

    return event

def lambda_handler(event, context):
    body = _parse_body(event)
    text = body.get("inputs", "")

    if not isinstance(text, str) or not text.strip():
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": "Empty or missing 'inputs' received by Lambda",
                "raw_event_sample": str(event)[:500]
            })
        }

    payload = {
        "inputs": text,
        "parameters": {
            "max_new_tokens": 128,
            "temperature": 0.0,
            "top_p": 0.9,
            "do_sample": False,
            "return_full_text": False
        }
    }

    item_id = f"{int(time.time()*1000)}#{context.aws_request_id}"

    try:
        resp = runtime.invoke_endpoint(
            EndpointName=ENDPOINT,
            ContentType="application/json",
            Accept="application/json",
            Body=json.dumps(payload).encode("utf-8"),
        )

        raw_out = resp["Body"].read().decode("utf-8", errors="replace")
        result = try_parse_json(raw_out)

        # Log success
        log_item = {
            "id": item_id,                 # REQUIRED partition key
            "request_id": item_id,         # optional
            "status": "ok",
            "prompt": text[:4000],
            "response": safe_json(result),
            "timestamp": int(time.time())
        }
        dynamo.put_item(Item=log_item)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"result": result})
        }

    except Exception as e:
        # Log failure (ALSO must include 'id')
        log_item = {
            "id": item_id,                 # REQUIRED partition key
            "request_id": item_id,
            "status": "error",
            "prompt": text[:4000],
            "response": safe_json({"invoke_error": str(e)}),
            "timestamp": int(time.time())
        }
        dynamo.put_item(Item=log_item)

        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }