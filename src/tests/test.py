import os
import sys
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from copy import deepcopy

import pandas as pd
from dotenv import load_dotenv

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from src.sales import create_sales_dataframe
from src.sheets import (
    authorize,
    add_sheet,
    modify_sales_dataframe,
    get_done_invoices,
    get_cancelled_invoices,
    get_invoice_numbers,
    get_invoice_links,
    write_to_sheet,
    format_sheet,
    clear_cancellations_range
    )
from src.utils import month_to_spanish, get_invoice_num_formula

load_dotenv()
APP_ID = os.getenv("APP_ID")
SECRET_KEY = os.getenv("SECRET_KEY")
USER_ID = os.getenv("USER_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
EXPIRATION_DATE = datetime.fromisoformat(os.getenv("EXPIRATION_DATE"))
SPREADSHEET_ID = os.getenv("TEST_SPREADSHEET_ID")
A_INVOICES_FOLDER_ID = os.getenv("A_INVOICES_FOLDER_ID")
B_INVOICES_FOLDER_ID = os.getenv("B_INVOICES_FOLDER_ID")

BS_AS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def create_cancellations_dataframe(info_df: pd.DataFrame, service, spreadsheet_id: str, sheet_name: str, sheet_id: int, test_num) -> pd.DataFrame | None:
    
    indices = []
    cancellations_dates = []
    customers_info = []
    invoices_numbers = []
    
    with open(f"src/tests/test_sales/test_september_{test_num}.json", "r+", encoding="utf-8") as f:
        d = json.load(f)
        
        pending_cancellations = d["info"]["pending_cancellations"]
        
        cancelled_invoices = get_cancelled_invoices(service, spreadsheet_id, max(len(pending_cancellations) + 2, 3), sheet_name)
        
        clear_cancellations_range(service, spreadsheet_id, sheet_name, sheet_id, len(pending_cancellations) + 2)
        
        cancelled_indices = deepcopy(d["info"]["cancelled_indices"])
        
        new_cancelled_indices = []
        for idx, cancelled in enumerate(cancelled_invoices):
            if cancelled:
                cancelled_indices.append(pending_cancellations[idx])
                new_cancelled_indices.append(pending_cancellations[idx])
        
        d["info"]["cancelled_indices"].extend(new_cancelled_indices)
                 
        for row in range(len(info_df)):
            if info_df.at[row, "invoice_done"] and info_df.at[row, "cancelled"] and row not in cancelled_indices:
                indices.append(row)
                cancellations_dates.append(info_df.at[row, "cancellation_date"])
                customers_info.append(info_df.at[row, "customer_info"])
                invoices_numbers.append(info_df.at[row, "invoice_number"])
        
        cancellations_df = pd.DataFrame({"indices" : indices,
                                         "cancellation_date" : cancellations_dates,
                                         "customer_info" : customers_info,
                                         "invoice_number" : invoices_numbers})
        
        if len(cancellations_df) != 0:
            cancellations_df["cancellation_date"] = pd.to_datetime(cancellations_df["cancellation_date"])
            cancellations_df["cancellation_date"] = cancellations_df["cancellation_date"].dt.tz_convert("America/Argentina/Buenos_Aires")
            cancellations_df = cancellations_df.sort_values(by="cancellation_date")
            cancellations_df["cancellation_date"] = cancellations_df["cancellation_date"].dt.strftime("%d/%m/%y")
            
            d["info"]["pending_cancellations"] = cancellations_df["indices"].values.tolist()
        else:
            d["info"]["pending_cancellations"] = []
        
        f.seek(0)
        f.truncate(0)
        json.dump(d, f, indent=2)
    
    if len(cancellations_df) == 0:
        return None
        
    cancellations_df["invoice_cancelled"] = False
    cancellations_df = cancellations_df[["cancellation_date", "invoice_cancelled", "customer_info", "invoice_number"]]
    cancellations_df = cancellations_df.rename(columns={
        "cancellation_date" : "FECHA CANCELACIÓN",
        "invoice_cancelled" : "FACTURA ANULADA",
        "customer_info" : "DATOS CLIENTE",
        "invoice_number" : "Nº FACTURA"
    })
    
    return cancellations_df

def test(record, test_num) -> None:
    month_int = 9
    month_spanish = month_to_spanish(month_int)

    sheet_id = month_int - 1
    sheet_name = f"Daniel - {month_spanish[:3].upper()}"

    sales_df = create_sales_dataframe(record)
    last_row_sales = len(sales_df) + 2

    try:
        sheets_service, drive_service = authorize()
    except RefreshError:
        os.remove("google_creds/token.json")
        sheets_service, drive_service = authorize()

    try:
        add_sheet(sheets_service, SPREADSHEET_ID, sheet_id, sheet_name)
        sales_df, _ = modify_sales_dataframe(sales_df)
        cancellations_df = None
    except HttpError:
        done_invoices = get_done_invoices(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_name)
        invoice_numbers = get_invoice_numbers(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_name)

        a_invoice_links = get_invoice_links(drive_service, A_INVOICES_FOLDER_ID)
        b_invoice_links = get_invoice_links(drive_service, B_INVOICES_FOLDER_ID)

        invoice_links = []
        a_invoice_index = 0
        b_invoice_index = 0
        for idx, (invoice, done) in enumerate(zip(invoice_numbers, done_invoices)):
            if done:
                if invoice[-1] == "A":
                    while invoice[:-1] != a_invoice_links[a_invoice_index]["num"]:
                        a_invoice_index += 1
                    invoice_links.append(get_invoice_num_formula(url=a_invoice_links[a_invoice_index]["link"], num=invoice))
                else:
                    while invoice[:-1] != b_invoice_links[b_invoice_index]["num"]:
                        b_invoice_index += 1
                    invoice_links.append(get_invoice_num_formula(url=b_invoice_links[b_invoice_index]["link"], num=invoice))
            else:
                invoice_links.append(get_invoice_num_formula(row=idx+3, hyperlink=False))

        sales_df, cancellations_info_df = modify_sales_dataframe(sales_df, done_invoices, invoice_links)
        cancellations_df = create_cancellations_dataframe(cancellations_info_df, sheets_service, SPREADSHEET_ID, sheet_name, sheet_id, test_num)
        try:
            last_row_cancellations = len(cancellations_df) + 2
        except TypeError:
            pass

    sales = [[month_spanish.upper()]]
    sales.append(sales_df.columns.values.tolist())
    sales.extend(sales_df.values.tolist())

    try:
        cancellations = [cancellations_df.columns.values.tolist()]
        cancellations.extend(cancellations_df.values.tolist())
        write_to_sheet(sheets_service, SPREADSHEET_ID, sales, last_row_sales, sheet_name, cancellations, last_row_cancellations)
        format_sheet(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_id, last_row_cancellations)
    except AttributeError:
        write_to_sheet(sheets_service, SPREADSHEET_ID, sales, last_row_sales, sheet_name)
        format_sheet(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_id)

def main() -> None:
    test_num = sys.argv[1]
    
    with open(f"src/tests/test_sales/test_september_{test_num}.json", encoding="utf-8") as f:
        record = json.load(f)

    test(record["sales"], test_num)

if __name__ == "__main__":
    main()