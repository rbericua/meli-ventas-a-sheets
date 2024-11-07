from datetime import datetime, timedelta

import requests
import pandas as pd
from dotenv import find_dotenv, set_key

def refresh_token(app_id: int, secret_key: str, refresh_token: str) -> str:
    r = requests.post("https://api.mercadolibre.com/oauth/token",
           headers={"accept" : "application/json",
                    "content-type" : "application/x-www-form-urlencoded"},  
           json={"grant_type" : "refresh_token",
                 "client_id" : app_id,
                 "client_secret" : secret_key,
                 "refresh_token" : refresh_token})
    r = r.json()
    
    dotenv_path = find_dotenv()
    set_key(dotenv_path, "ACCESS_TOKEN", r["access_token"], "never")
    set_key(dotenv_path, "REFRESH_TOKEN", r["refresh_token"], "never")
    set_key(dotenv_path, "EXPIRATION_DATE", (datetime.now() + timedelta(hours=6)).isoformat(timespec="milliseconds"), "never")
    
    return r["access_token"]

def to_meli_date_format(date: str | datetime) -> str:
    if type(date) == str:
        return date
    elif type(date) == datetime:
        return date.isoformat(timespec="milliseconds")
    else:
        raise TypeError
    
def month_to_spanish(month: int | str) -> str:
    months = {"january" : "enero",
              "february" : "febrero",
              "march" : "marzo",
              "april" : "abril",
              "may" : "mayo",
              "june" : "junio",
              "july" : "julio",
              "august" : "agosto",
              "september" : "septiembre",
              "october" : "octubre",
              "november" : "noviembre",
              "december" : "diciembre"}

    if type(month) == int:
        return list(months.values())[month-1]
    elif type(month) == str:
        return months[month.lower()]
    else:
        raise TypeError(f"Unsupported type: {type(month)}")

def format_numbers(s: pd.Series) -> pd.Series:
    s = s.apply(str)
    s = s.str.replace(".0$", "", regex=True)
    return s

def get_invoice_num_formula(*, url: str | None = None, num: str | None = None, row: int | None = None, hyperlink: bool = True) -> str:
    if hyperlink:
        return f"=HYPERLINK(\"{url}\"; \"{num}\")"
    else:
        return f"=LET(num; XLOOKUP(D{row}; D$2:D{row-1}; J$2:J{row-1}; XLOOKUP(D{row}; INDIRECT(\"Daniel - \"&LEFT(TEXT(DATE(; MONTH(A$1&1)-1; 1); \"mmmm\"); 3)&\"!D:D\"); INDIRECT(\"Daniel - \"&LEFT(TEXT(DATE(; MONTH(A$1&1)-1; 1); \"mmmm\"); 3)&\"!J:J\");;; -1);; -1); IF(C{row}=C{row-1}; J{row-1}; IF(NOT(REGEXMATCH(D{row}; \"\([AB]\)\")); LEFT(num; LEN(num)-1)+1&D{row}; \"\")))"