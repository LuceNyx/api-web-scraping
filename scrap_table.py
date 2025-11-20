import requests
from bs4 import BeautifulSoup
import boto3
from decimal import Decimal

dynamo = boto3.resource('dynamodb')
table = dynamo.Table("TablaWebScrapping")

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    html = requests.get(url, timeout=10).text

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")

    results = []
    errors = []

    for r in rows:
        cols = r.find_all("td")
        if len(cols) < 6:
            continue

        try:
            data = {
                "id": cols[0].text.strip(),
                "fecha": cols[1].text.strip(),
                "hora": cols[2].text.strip(),
                "magnitud": Decimal(cols[3].text.strip().replace(",", ".")),
                "profundidad_km": Decimal(cols[4].text.strip().replace(" km", "").replace(",", ".")),
                "epicentro": cols[5].text.strip()
            }

            table.put_item(Item=data)

            results.append(data)

        except Exception as e:
            errors.append(f"Error al escribir en DynamoDB: {repr(e)}")

    # Verificación final contando elementos en tabla
    try:
        scan_resp = table.scan()
        table_count = len(scan_resp.get("Items", []))
    except:
        table_count = -1

    return {
        "statusCode": 200 if len(errors) == 0 else 500,
        "body": str({
            "fetched": len(rows),
            "saved": len(results),
            "table_after_save_count": table_count,
            "errors": errors,
            "sample_saved": results[:2]
        })
    }
