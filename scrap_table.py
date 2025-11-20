# scrap_table.py
import os
import json
import uuid
import requests
import boto3
import traceback
from decimal import Decimal, InvalidOperation
from datetime import datetime
from botocore.exceptions import ClientError

# Config (si quieres usar env vars, puedes definir TABLE_NAME en serverless.yml)
TABLE_NAME = os.environ.get("TABLE_NAME", "TablaWebScrapping")
dynamodb = boto3.resource("dynamodb")
table_db = dynamodb.Table(TABLE_NAME)

ARC_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

def to_decimal_safe(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, float):
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(v, str):
        s = v.strip().replace(" km", "").replace("km", "").replace(",", ".")
        s = s.replace("\u200b", "")
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return s
    try:
        return str(v)
    except Exception:
        return None

def convert_decimals(obj):
    """
    Convierte recursivamente Decimal -> float (u otro tipo JSON-serializable).
    Mantiene el resto de tipos intactos.
    """
    if isinstance(obj, Decimal):
        # convertimos a float para mantener tipo numérico en JSON
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(convert_decimals(v) for v in obj)
    # otros tipos (str, int, bool, None)
    return obj

def fetch_latest_sismos(limit=10):
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": str(limit),
        "f": "json"
    }
    resp = requests.get(ARC_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", []) or []
    items = []
    for feat in features:
        attr = feat.get("attributes", {}) or {}
        geom = feat.get("geometry", {}) or {}

        item = {}
        fecha_val = attr.get("fecha") or attr.get("Fecha")
        if isinstance(fecha_val, (int, float)):
            try:
                item["fecha_iso"] = datetime.utcfromtimestamp(fecha_val / 1000).isoformat() + "Z"
            except Exception:
                item["fecha_raw"] = str(fecha_val)
        else:
            item["fecha_raw"] = str(fecha_val) if fecha_val is not None else None

        mag = attr.get("mag") or attr.get("MAG") or attr.get("magnitud") or attr.get("magn")
        item["magnitud"] = to_decimal_safe(mag)

        prof = attr.get("profundidad") or attr.get("PROFUNDIDAD") or attr.get("depth") or attr.get("z")
        item["profundidad_km"] = to_decimal_safe(prof)

        item["referencia_texto"] = (attr.get("referencia") or attr.get("Referencia") or
                                    attr.get("ref") or attr.get("referencia_texto") or None)

        lat = geom.get("y") or attr.get("lat") or attr.get("LAT") or attr.get("latitude")
        lon = geom.get("x") or attr.get("lon") or attr.get("LON") or attr.get("longitude")
        item["latitude"] = to_decimal_safe(lat)
        item["longitude"] = to_decimal_safe(lon)

        item["report_id"] = (attr.get("OBJECTID") or attr.get("OBJECTID_1") or
                             attr.get("id") or attr.get("ID") or None)

        raw = {}
        for k, v in attr.items():
            if isinstance(v, float):
                raw[k] = to_decimal_safe(v)
            elif isinstance(v, (str, int, bool)) or v is None:
                raw[k] = v
            else:
                try:
                    raw[k] = str(v)
                except Exception:
                    raw[k] = None
        item["raw_attributes"] = raw

        items.append(item)
    return items

def clean_item_for_dynamo(item):
    return {k: v for k, v in item.items() if v is not None}

def lambda_handler(event, context):
    result = {"fetched": 0, "saved": 0, "table_after_save_count": 0, "errors": [], "sample_saved": []}

    try:
        sismos = fetch_latest_sismos(limit=10)
        result["fetched"] = len(sismos)
        if not sismos:
            result["errors"].append("No se obtuvieron sismos desde ArcGIS")
            serial = convert_decimals(result)
            return {"statusCode": 404, "body": json.dumps(serial, ensure_ascii=False)}
    except Exception as e:
        tb = traceback.format_exc()
        result["errors"].append("Error fetch: " + repr(e))
        result["errors"].append(tb)
        serial = convert_decimals(result)
        return {"statusCode": 500, "body": json.dumps(serial, ensure_ascii=False)}

    try:
        # borrar items previos (si existen)
        try:
            scan_resp = table_db.scan(ProjectionExpression="id")
            existing = scan_resp.get("Items", []) or []
            if existing:
                with table_db.batch_writer() as batch:
                    for it in existing:
                        if "id" in it:
                            batch.delete_item(Key={"id": it["id"]})
        except Exception as e_scan:
            result["errors"].append("Warn scan/delete: " + repr(e_scan))

        for idx, s in enumerate(sismos, start=1):
            item = dict(s)
            item["id"] = str(uuid.uuid4())
            item["numero"] = idx
            cleaned = clean_item_for_dynamo(item)
            table_db.put_item(Item=cleaned)
            result["saved"] += 1
            if len(result["sample_saved"]) < 5:
                result["sample_saved"].append(cleaned)

        try:
            after = table_db.scan(Limit=20)
            result["table_after_save_count"] = len(after.get("Items", []) or [])
        except Exception as e_after:
            result["errors"].append("Warn scan after: " + repr(e_after))

    except ClientError as ce:
        tb = traceback.format_exc()
        result["errors"].append("Dynamo ClientError: " + repr(ce))
        result["errors"].append(tb)
        serial = convert_decimals(result)
        return {"statusCode": 500, "body": json.dumps(serial, ensure_ascii=False)}
    except Exception as e:
        tb = traceback.format_exc()
        result["errors"].append("Write error: " + repr(e))
        result["errors"].append(tb)
        serial = convert_decimals(result)
        return {"statusCode": 500, "body": json.dumps(serial, ensure_ascii=False)}

    # convertir Decimals antes de loguear / devolver
    serializable_result = convert_decimals(result)
    print("[lambda] resultado final:", json.dumps(serializable_result, ensure_ascii=False))
    return {"statusCode": 200, "body": json.dumps(serializable_result, ensure_ascii=False)}
