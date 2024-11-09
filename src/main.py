import os
import sys
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from calendar import monthrange

import requests
from dotenv import load_dotenv

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from src.sales import update_json, create_sales_dataframe
from src.sheets import (
    authorize,
    add_sheet,
    modify_sales_dataframe,
    get_done_invoices,
    get_invoice_numbers,
    get_invoice_links,
    create_cancellations_dataframe,
    write_to_sheet,
    format_sheet
    )
from src.utils import refresh_token, month_to_spanish, get_invoice_num_formula

load_dotenv()
APP_ID = os.getenv("APP_ID")
SECRET_KEY = os.getenv("SECRET_KEY")
USER_ID = os.getenv("USER_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
EXPIRATION_DATE = datetime.fromisoformat(os.getenv("EXPIRATION_DATE"))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
A_INVOICES_FOLDER_ID = os.getenv("A_INVOICES_FOLDER_ID")
B_INVOICES_FOLDER_ID = os.getenv("B_INVOICES_FOLDER_ID")

BS_AS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def get_month() -> tuple[datetime, datetime]:
    month = sys.argv[1:]
    
    months = ["", "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
    now = datetime.now(tz=BS_AS_TZ)
    
    if len(month) == 0: # current month
        end = now
        start = datetime(end.year, end.month, 1, tzinfo=BS_AS_TZ)
    elif len(month) == 1:
        month = month[0] 
        if month == "prev": # previous month
            end = datetime(now.year, now.month, 1, tzinfo=BS_AS_TZ) - timedelta(milliseconds=1)
            start = datetime(end.year, end.month, 1, tzinfo=BS_AS_TZ)
        elif re.match(r"^[a-z]{3}$", month):
            m = months.index(month)
            y = now.year if m <= now.month else now.year - 1
            start = datetime(y, m, 1, tzinfo=BS_AS_TZ)
            end = datetime(y, m, monthrange(y, m)[1], 23, 59, 59, 999000, tzinfo=BS_AS_TZ) if m != now.month else now
        else:
            raise ValueError("Fecha inválida. Ingrese una fecha así: prev, mmm, mmm yy")
    elif len(month) == 2:
        m, y = month
        m = months.index(m)
        y = 2000 + int(y)
        if (y == now.year and m > now.month) or y > now.year:
            raise ValueError("Esa fecha todavía no llegó")
        start = datetime(y, m, 1, tzinfo=BS_AS_TZ)
        end = datetime(y, m, monthrange(y, m)[1], 23, 59, 59, 999000, tzinfo=BS_AS_TZ) if m != now.month else now
    else:
        raise ValueError("Fecha inválida. Ingrese una fecha así: prev, mmm, mmm yy")
    
    return start, end

def main(start: datetime, end: datetime) -> None:
    s = requests.Session()
    s.headers = {"Authorization" : f"Bearer {ACCESS_TOKEN}"}

    if datetime.now() > EXPIRATION_DATE:
        s.headers.update({"Authorization" : f"Bearer {refresh_token(APP_ID, SECRET_KEY, REFRESH_TOKEN)}"})

    month_int = start.month
    month_spanish = month_to_spanish(month_int)

    sheet_id = month_int - 1
    sheet_name = f"Daniel - {month_spanish[:3].upper()}"

    record = update_json(s, USER_ID, start, end)
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
    except HttpError as e:
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
        cancellations_df = create_cancellations_dataframe(cancellations_info_df, sheets_service, SPREADSHEET_ID, sheet_name, sheet_id, start)

    sales = [[month_spanish.upper()]]
    sales.append(sales_df.columns.values.tolist())
    sales.extend(sales_df.values.tolist())

    if cancellations_df:
        last_row_cancellations = len(cancellations_df) + 2
        cancellations = [cancellations_df.columns.values.tolist()]
        cancellations.extend(cancellations_df.values.tolist())
        write_to_sheet(sheets_service, SPREADSHEET_ID, sales, last_row_sales, sheet_name, cancellations, last_row_cancellations)
        format_sheet(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_id, last_row_cancellations)
    else:
        write_to_sheet(sheets_service, SPREADSHEET_ID, sales, last_row_sales, sheet_name)
        format_sheet(sheets_service, SPREADSHEET_ID, last_row_sales, sheet_id)

if __name__ == "__main__":
    start, end = get_month()
    main(start, end)