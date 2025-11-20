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

# Nombre de la tabla (tu serverless.yml crea TablaWebScrapping por defecto)
TABLE_NAME = os.environ.get("TABLE_NAME", "TablaWebScrapping")
# boto3 usará la región que tenga la Lambda si no se especifica
dynamodb = boto3.resource("dynamodb")
table_db = dynamodb.Table(TABLE_NAME)

ARC_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

print(f"[scrap_table] módulo cargado. TABLE_NAME={TABLE_NAME}")

def to_decimal_safe(v):
    """Convierte int/float/str a Decimal cuando sea posible; deja otros tipos intactos."""
    if v is None:
        return None
    # Si ya es Decimal
    if isinstance(v, Decimal):
        return v
    # Int -> Decimal directo
    if isinstance(v, int):
        return Decimal(v)
    # Float -> usar str para evitar imprecisiones
    if isinstance(v, float):
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError):
            return None
    # Si viene como string, limpiar y convertir si parece número
    if isinstance(v, str):
        s = v.strip()
        # quitar unidades comunes y comas de miles, reemplazar coma decimal por punto
        s = s.replace(" km", "").replace("km", "").replace(",", ".")
        s = s.replace("\u200b", "")  # eliminar caracteres invisibles si existieran
        # intentar convertir a Decimal
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            # no es número -> devolver la cadena original
            return s
    # otros tipos (bool, dict, list...) -> devolver string seguro
    try:
        return str(v)
    except Exception:
        return None

def fetch_latest_sismos(limit=10):
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": str(limit),
        "f": "json"
    }
    print(f"[fetch] Llamando ArcGIS: {ARC_URL} params={params}")
    resp = requests.get(ARC_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", []) or []
    print(f"[fetch] features obtenidas: {len(features)}")
    items = []
    for feat in features:
        attr = feat.get("attributes", {}) or {}
        geom = feat.get("geometry", {}) or {}

        item = {}

        # Fecha (normalmente epoch ms en campo 'fecha')
        fecha_val = attr.get("fecha") or attr.get("Fecha")
        if isinstance(fecha_val, (int, float)):
            try:
                item["fecha_iso"] = datetime.utcfromtimestamp(fecha_val / 1000).isoformat() + "Z"
            except Exception:
                item["fecha_raw"] = str(fecha_val)
        else:
            item["fecha_raw"] = str(fecha_val) if fecha_val is not None else None

        # Magnitud
        mag = attr.get("mag") or attr.get("MAG") or attr.get("magnitud") or attr.get("magn")
        item["magnitud"] = to_decimal_safe(mag)

        # Profundidad (km)
        prof = attr.get("profundidad") or attr.get("PROFUNDIDAD") or attr.get("depth") or attr.get("z")
        item["profundidad_km"] = to_decimal_safe(prof)

        # Referencia textual / lugar
        item["referencia_texto"] = (attr.get("referencia") or attr.get("Referencia") or
                                    attr.get("ref") or attr.get("referencia_texto") or None)

        # Lat / Lon: geometry o atributos
        lat = geom.get("y") or attr.get("lat") or attr.get("LAT") or attr.get("latitude")
        lon = geom.get("x") or attr.get("lon") or attr.get("LON") or attr.get("longitude")
        item["latitude"] = to_decimal_safe(lat)
        item["longitude"] = to_decimal_safe(lon)

        # report id (OBJECTID u otro)
        item["report_id"] = (attr.get("OBJECTID") or attr.get("OBJECTID_1") or
                             attr.get("id") or attr.get("ID") or None)

        # Guardar atributos crudos (con tipos simples o strings)
        raw = {}
        for k, v in attr.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                # Para floats convertir a Decimal
                if isinstance(v, float):
                    raw[k] = to_decimal_safe(v)
                else:
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
    """Eliminar keys con valor None (DynamoDB no guarda None)."""
    return {k: v for k, v in item.items() if v is not None}

def lambda_handler(event, context):
    result = {"fetched": 0, "saved": 0, "table_after_save_count": 0, "errors": [], "sample_saved": []}
    print("[lambda] inicio")

    try:
        sismos = fetch_latest_sismos(limit=10)
        result["fetched"] = len(sismos)
        if not sismos:
            msg = "No se obtuvieron sismos desde ArcGIS"
            print("[lambda] " + msg)
            result["errors"].append(msg)
            return {"statusCode": 404, "body": json.dumps(result, ensure_ascii=False)}
    except Exception as e:
        tb = traceback.format_exc()
        print("[lambda] ERROR fetch:", repr(e), tb)
        result["errors"].append("Error fetch: " + repr(e))
        result["errors"].append(tb)
        return {"statusCode": 500, "body": json.dumps(result, ensure_ascii=False)}

    # Guardar en DynamoDB: borrado previo solo si hay sismos
    try:
        # borrar existentes (opcional)
        try:
            scan_resp = table_db.scan(ProjectionExpression="id")
            existing = scan_resp.get("Items", []) or []
            print(f"[lambda] items existentes en tabla: {len(existing)}")
            if existing:
                with table_db.batch_writer() as batch:
                    for it in existing:
                        if "id" in it:
                            batch.delete_item(Key={"id": it["id"]})
                print("[lambda] borrado previo completado")
        except Exception as e_scan:
            # registramos y continuamos (pero es indicio de permisos/región)
            print("[lambda] WARN al scan/borrar:", repr(e_scan))
            result["errors"].append("Warn scan/delete: " + repr(e_scan))

        # insertar nuevos
        for idx, s in enumerate(sismos, start=1):
            item = dict(s)
            item["id"] = str(uuid.uuid4())
            item["numero"] = idx
            cleaned = clean_item_for_dynamo(item)
            # DynamoDB acepta Decimal para números: ya convertimos floats con to_decimal_safe
            table_db.put_item(Item=cleaned)
            result["saved"] += 1
            if len(result["sample_saved"]) < 5:
                result["sample_saved"].append(cleaned)

        # scan final para verificar
        try:
            after = table_db.scan(Limit=20)
            result["table_after_save_count"] = len(after.get("Items", []) or [])
        except Exception as e_after:
            print("[lambda] WARN al scan AFTER:", repr(e_after))
            result["errors"].append("Warn scan after: " + repr(e_after))

    except ClientError as ce:
        tb = traceback.format_exc()
        print("[lambda] ClientError DynamoDB:", repr(ce), tb)
        result["errors"].append("Dynamo ClientError: " + repr(ce))
        result["errors"].append(tb)
        return {"statusCode": 500, "body": json.dumps(result, ensure_ascii=False)}
    except Exception as e:
        tb = traceback.format_exc()
        print("[lambda] ERROR escritura:", repr(e), tb)
        result["errors"].append("Write error: " + repr(e))
        result["errors"].append(tb)
        return {"statusCode": 500, "body": json.dumps(result, ensure_ascii=False)}

    print("[lambda] resultado final:", json.dumps(result, ensure_ascii=False))
    return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False)}
