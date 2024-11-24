import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from src.utils import to_meli_date_format

BS_AS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def get_sales(s: requests.Session, user_id: int, start: datetime, end: datetime, offset: int = 0, cancelled: bool = False) -> list:
    url = "https://api.mercadolibre.com/orders/search"
    params = {"seller" : user_id,
              "order.date_closed.from" : to_meli_date_format(start), # TODO: date_created o date_closed?
              "order.date_closed.to": to_meli_date_format(end),
              "offset" : offset}
    
    if cancelled:
        params.update({"order.status" : "cancelled"})
        
    r = s.get(url=url, params=params)
 
    return r.json()["results"]

def get_buyer_info(s: requests.Session, sale_id: int) -> dict:
    r = s.get(f"https://api.mercadolibre.com/orders/{sale_id}/billing_info",
              headers={"X-Version" : "2"})
    return r.json()["buyer"]["billing_info"]    

def create_record(s: requests.Session, user_id: int, start: datetime, end: datetime) -> list:    
    sales = []
    offset = 0
    while True:
        sales_batch = get_sales(s, user_id, start, end, offset)
        sales.extend(sales_batch)
        if len(sales_batch) < 51:
            break
        offset += 51
            
    record = []
    for sale in sales:
        try:
            id = sale["id"]
            cancelled = True if sale["status"] == "cancelled" else False
            cancellation_date = sale["cancel_detail"]["date"] if cancelled else None
            sale_date = sale["date_closed"]
            product = sale["order_items"][0]["item"]["title"]
            quantity = sale["order_items"][0]["quantity"]
            unit_price = sale["order_items"][0]["unit_price"]
            shipping_cost = sale["paid_amount"] - sale["total_amount"] if not cancelled else sale["payments"][0]["shipping_cost"]
            total = sale["paid_amount"] if not cancelled else unit_price * quantity + shipping_cost
            
            buyer = get_buyer_info(s, id)
            name = f"{buyer["name"]} {buyer["last_name"]}" if "last_name" in buyer else buyer["name"]
            identification = f"{buyer["identification"]["type"]} {buyer["identification"]["number"]}"
            tax_status = buyer["taxes"]["taxpayer_type"]["description"]
            address = f"{buyer["address"]["street_name"]} {buyer["address"]["street_number"]}, {buyer["address"]["city_name"]} - C.P.: {buyer["address"]["zip_code"]}, {buyer["address"]["state"]["name"]}"
            jurisdiction = buyer["address"]["state"]["name"]
            
            if len(sale["payments"]) > 1:
                print(f"Sale {id} has more than 1 payment")
            if len(sale["order_items"]) > 1:
                print(f"Sale {id} has more than 1 item")
        except Exception as e:
            print(f"{str(e)}\nSale ID: {id}")
            with open(f"meli/json/{id}.json", "w", encoding="utf-8") as f:
                json.dump(buyer, f, int=2)
            raise Exception(e)
            
        record.append({
            "id" : id,
            "cancelled" : cancelled,
            "cancellation_date" : cancellation_date,
            "sale_date" : sale_date,
            "product" : product,
            "total" : total,
            "quantity" : quantity,
            "unit_price" : unit_price,
            "shipping_cost" : shipping_cost,
            "name" : name,
            "identification" : identification,
            "tax_status" : tax_status,
            "address" : address,
            "jurisdiction" : jurisdiction
        })
    
    return record

def update_cancelled(s: requests.Session, user_id: int, sales: list, start: datetime, end: datetime) -> list:
    cancelled_sales = get_sales(s, user_id, start, end, cancelled=True)
    cancelled_ids = [sale["id"] for sale in cancelled_sales]

    cancelled_idx = 0
    for sale in sales:
        if sale["id"] in cancelled_ids:
            sale["cancelled"] = True
            sale["cancellation_date"] = cancelled_sales[cancelled_idx]["cancel_detail"]["date"]
            cancelled_idx += 1
    
    return sales

def update_json(s: requests.Session, user_id: int, start: datetime, end: datetime) -> list:
    now = datetime.now(tz=BS_AS_TZ)
    month = start.strftime("%B_%y").lower()
    
    try:
        with open(f"sales_db/{month}.json", "r+", encoding="utf-8") as f:
            d = json.load(f)
            date_last_updated = d["info"]["date_last_updated"]
            
            if datetime.fromisoformat(date_last_updated) >= end:
                d["sales"] = update_cancelled(s, user_id, d["sales"], start, end)
            else:   
                d["sales"] = update_cancelled(s, user_id, d["sales"], start, date_last_updated)
                record = create_record(s, user_id, date_last_updated, end)
                d["sales"].extend(record)

            d["info"]["date_last_updated"] = to_meli_date_format(now)
            
            f.seek(0)
            f.truncate(0)
            json.dump(d, f, indent=2)    
    except FileNotFoundError:
        record = create_record(s, user_id, start, end)
        
        d = {"info" : {"date_last_updated" : to_meli_date_format(now),
                       "pending_cancellations" : [],
                       "cancelled_indices" : []},
             "sales" : record}        
        
        with open(f"sales_db/{month}.json", "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
        
    return d["sales"]

def create_sales_dataframe(record: list) -> pd.DataFrame:
    df = pd.DataFrame.from_records(record)
    
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["sale_date"] = df["sale_date"].dt.tz_convert("America/Argentina/Buenos_Aires")
    df["sale_date"] = df["sale_date"].dt.strftime("%d/%m/%y")
    
    df["customer_info"] = df["name"] + " - " + df["identification"] + "\n" + df["address"] + "\n" + df["tax_status"]
    
    df["invoice_type"] = np.where(df["tax_status"].isin(["Monotributo", "IVA Responsable Inscripto"]), "A", "B")
    df["unit_price"] = np.where(df["invoice_type"] == "A", round(df["unit_price"] / 1.21, 2), df["unit_price"])
    df["shipping_cost"] = np.where(df["invoice_type"] == "A", round(df["shipping_cost"] / 1.21, 2), df["shipping_cost"])
    
    return df