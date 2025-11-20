# scrap_table.py
import os
import json
import uuid
import requests
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Config por defecto (tu serverless.yml no define env vars; mantengo defaults)
TABLE_NAME = os.environ.get("TABLE_NAME", "TablaWebScrapping")
# boto3 usará la región configurada por Lambda/Serverless; si quieres forzar una, define AWS_REGION env
REGION = os.environ.get("AWS_REGION", None)

dynamodb = boto3.resource("dynamodb", region_name=REGION) if REGION else boto3.resource("dynamodb")
table_db = dynamodb.Table(TABLE_NAME)

# Endpoint ArcGIS "Sismos Reportados" del IGP (feature layer)
ARC_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

def fetch_latest_sismos(limit=10):
    """Consulta ArcGIS y devuelve una lista de items normalizados (máximo `limit`)."""
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": str(limit),
        "f": "json"
    }
    resp = requests.get(ARC_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", []) or []
    items = []
    for feat in features:
        attr = feat.get("attributes", {}) or {}
        geom = feat.get("geometry", {}) or {}

        item = {}
        # Fecha: suele venir en ms desde epoch en campo 'fecha'
        fecha_val = attr.get("fecha") or attr.get("Fecha") or attr.get("FECHA")
        if isinstance(fecha_val, (int, float)):
            try:
                item["fecha_iso"] = datetime.utcfromtimestamp(fecha_val / 1000).isoformat() + "Z"
            except Exception:
                item["fecha_raw"] = str(fecha_val)
        else:
            item["fecha_raw"] = str(fecha_val) if fecha_val is not None else None

        # Magnitud
        item["magnitud"] = (attr.get("mag") or attr.get("MAG") or
                            attr.get("magnitud") or attr.get("MAGNITUD") or attr.get("magn"))

        # Profundidad (km)
        item["profundidad_km"] = (attr.get("profundidad") or attr.get("PROFUNDIDAD") or
                                  attr.get("depth") or attr.get("z"))

        # Referencia textual / lugar
        item["referencia_texto"] = (attr.get("referencia") or attr.get("Referencia") or
                                    attr.get("ref") or attr.get("referencia_texto") or None)

        # Lat / Lon desde geometry o atributos
        lat = geom.get("y") or attr.get("lat") or attr.get("LAT") or attr.get("latitude")
        lon = geom.get("x") or attr.get("lon") or attr.get("LON") or attr.get("longitude")
        item["latitude"] = lat
        item["longitude"] = lon

        # ID report (OBJECTID u otro)
        item["report_id"] = (attr.get("OBJECTID") or attr.get("OBJECTID_1") or
                             attr.get("id") or attr.get("ID") or attr.get("ref") or None)

        # Guardar raw attributes (convertir valores no-serializables a str)
        raw_attrs = {}
        for k, v in attr.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                raw_attrs[k] = v
            else:
                try:
                    raw_attrs[k] = str(v)
                except Exception:
                    raw_attrs[k] = None
        item["raw_attributes"] = raw_attrs

        items.append(item)
    return items

def clean_item_for_dynamo(item):
    """Quita valores None para evitar errores al insertar en DynamoDB."""
    return {k: v for k, v in item.items() if v is not None}

def lambda_handler(event, context):
    result = {
        "fetched": 0,
        "saved": 0,
        "table_after_save_count": 0,
        "errors": [],
        "sample_saved": []
    }

    print("Inicio de ejecución: obteniendo últimos sismos desde ArcGIS...")
    try:
        sismos = fetch_latest_sismos(limit=10)
        result["fetched"] = len(sismos)
        print(f"Sismos obtenidos: {result['fetched']}")
        if not sismos:
            result["errors"].append("No se obtuvieron sismos desde el servicio ArcGIS.")
            return {"statusCode": 404, "body": json.dumps(result, ensure_ascii=False)}
    except Exception as e:
        err = f"Error al obtener datos desde ArcGIS: {repr(e)}"
        print(err)
        result["errors"].append(err)
        return {"statusCode": 500, "body": json.dumps(result, ensure_ascii=False)}

    # Guardar en DynamoDB (borrado previo solo si se obtuvieron nuevos sismos)
    try:
        # Borrar items anteriores (si existen)
        try:
            scan_resp = table_db.scan(ProjectionExpression="id")
            existing = scan_resp.get("Items", []) or []
            if existing:
                print(f"Eliminando {len(existing)} items previos de la tabla {TABLE_NAME}...")
                with table_db.batch_writer() as batch:
                    for it in existing:
                        if "id" in it:
                            batch.delete_item(Key={"id": it["id"]})
        except ClientError as ce:
            # Si no se puede scanear, registramos y continuamos intentando escribir (pero prob. fallará)
            err = f"ClientError al scanear tabla DynamoDB: {repr(ce)}"
            print(err)
            result["errors"].append(err)
            # No retornamos aún; intentaremos escribir, y si falla, capturaremos después.

        # Insertar los nuevos sismos
        for idx, s in enumerate(sismos, start=1):
            item = dict(s)
            item["id"] = str(uuid.uuid4())
            item["numero"] = idx
            # Limpiar None
            item_clean = clean_item_for_dynamo(item)
            # DynamoDB no acepta tipos como NaN o objetos complejos; aquí asumimos strings/numeros simples
            table_db.put_item(Item=item_clean)
            result["saved"] += 1
            if result["saved"] <= 5:
                result["sample_saved"].append(item_clean)

        # Verificar con scan (limit para no traer todo)
        try:
            after = table_db.scan(Limit=20)
            result["table_after_save_count"] = len(after.get("Items", []) or [])
        except ClientError as ce2:
            err2 = f"ClientError después de insertar (scan): {repr(ce2)}"
            print(err2)
            result["errors"].append(err2)

    except Exception as e:
        err = f"Error al escribir en DynamoDB: {repr(e)}"
        print(err)
        result["errors"].append(err)
        return {"statusCode": 500, "body": json.dumps(result, ensure_ascii=False)}

    print("Ejecución finalizada:", json.dumps(result, ensure_ascii=False))
    return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False)}
