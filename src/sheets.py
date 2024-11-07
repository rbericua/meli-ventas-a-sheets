import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from copy import deepcopy

import pandas as pd
import numpy as np

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.utils import format_numbers, get_invoice_num_formula

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

BS_AS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def modify_sales_dataframe(df: pd.DataFrame, done_invoices: list | None = None, invoice_links: list | None = None) -> pd.DataFrame:
    df["customer_info"] = np.where(df["cancelled"], "CANCELADA\n" + df["customer_info"], df["customer_info"])
    
    if done_invoices:
        done_invoices.extend([False for _ in range(len(df) - len(done_invoices))])
        df["invoice_done"] = [True if row else False for row in done_invoices]
    else:
        df["invoice_done"] = False
    
    df["invoice_type"] = np.where(df["cancelled"] & ~df["invoice_done"], "(" + df["invoice_type"] + ")", df["invoice_type"])
        
    df["unit_price"] = df["unit_price"].replace(0.00, "")
    df["shipping_cost"] = df["shipping_cost"].replace(0.00, "")
    
    df["unit_price"] = format_numbers(df["unit_price"])
    df["shipping_cost"] = format_numbers(df["shipping_cost"])
    
    if invoice_links:
        invoice_links.extend([get_invoice_num_formula(row=row+3, hyperlink=False) for row in range(len(invoice_links), len(df))])
    else:
        invoice_links = [get_invoice_num_formula(row=row+3, hyperlink=False) for row in range(len(df))]
    df["invoice_number"] = invoice_links
        
    sales_df = df[["sale_date", "invoice_done", "customer_info", "invoice_type", "product", "quantity", "unit_price", "shipping_cost", "total", "invoice_number", "jurisdiction"]]
    sales_df = sales_df.rename(columns={
        "sale_date" : "FECHA VENTA",
        "invoice_done" : "FACTURA EMITIDA",
        "customer_info" : "DATOS CLIENTE",
        "invoice_type" : "TIPO FACTURA",
        "product" : "PRODUCTO",
        "quantity" : "UNIDADES",
        "unit_price" : "PRECIO UNITARIO",
        "shipping_cost" : "ENVIO",
        "total" : "TOTAL",
        "invoice_number" : "Nº FACTURA",
        "jurisdiction" : "JURISDICCION"
    })
    
    cancellations_info_df = df[["invoice_done", "cancelled", "customer_info", "invoice_number", "cancellation_date"]]

    return sales_df, cancellations_info_df

def create_cancellations_dataframe(info_df: pd.DataFrame, service, spreadsheet_id: str, sheet_name: str, sheet_id: int, start: datetime) -> pd.DataFrame | None:
    month = start.strftime("%B_%y").lower()
    
    indices = []
    cancellations_dates = []
    customers_info = []
    invoices_numbers = []
    
    with open(f"sales_db/{month}.json", "r+", encoding="utf-8") as f:
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

def authorize():
    creds = None
    if os.path.exists("google_creds/token.json"):
        creds = Credentials.from_authorized_user_file("google_creds/token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "google_creds/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("google_creds/token.json", "w") as token:
            token.write(creds.to_json())
    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return sheets_service, drive_service

def add_sheet(service, spreadsheet_id: str, sheet_id: int, sheet_name: str) -> None:
    body = {
        "requests" : [
            {
                "addSheet" : {
                    "properties" : {
                        "sheetId" : sheet_id,
                        "title" : sheet_name
                    }
                }
            }
        ]
    }
    
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                       body=body).execute()

def write_to_sheet(service, spreadsheet_id: str, sales: list, last_row_sales: int, sheet_name: str, cancellations: list | None = None, last_row_cancellations: int | None = None) -> None:
    body = {
		"valueInputOption" : "USER_ENTERED",
        "data" : [
            {
                "range" : f"'{sheet_name}'!A1:K{last_row_sales}",
                "values" : sales
            }
        ]
	}

    if cancellations:
        body["data"].append({
            "range" : f"'{sheet_name}'!M2:P{last_row_cancellations}",
            "values" : cancellations
        })

    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id,
                                                body=body).execute()

def get_done_invoices(service, spreadsheet_id: str, last_row: int, sheet_name: str) -> list:
    r = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                            range=f"'{sheet_name}'!B3:B{last_row}",
                                            majorDimension="COLUMNS",
                                            valueRenderOption="UNFORMATTED_VALUE").execute()
    
    try:
        return r["values"][0]
    except KeyError:
        return []

def get_invoice_numbers(service, spreadsheet_id: str, last_row: int, sheet_name: str) -> list:
    r = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                            range=f"'{sheet_name}'!J3:J{last_row}",
                                            majorDimension="COLUMNS",
                                            valueRenderOption="UNFORMATTED_VALUE").execute()
    
    try:
        return r["values"][0]
    except KeyError:
        return []

def get_cancelled_invoices(service, spreadsheet_id: str, last_row: int, sheet_name: str) -> list:
    r = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                            range=f"'{sheet_name}'!N3:N{last_row}",
                                            majorDimension="COLUMNS",
                                            valueRenderOption="UNFORMATTED_VALUE").execute()
    
    try:
        return r["values"][0]
    except KeyError:
        return []

def get_invoice_links(service, folder_id: str) -> list:
    r = service.files().list(q=f"'{folder_id}' in parents",
                            orderBy="name_natural desc",
                            fields="files(name, webViewLink)").execute()
    
    invoice_links = r["files"]
    invoice_links = [{"num" : invoice["name"][-7:-4],
                      "link" : invoice["webViewLink"]}
                     for invoice in invoice_links[::-1]]
    
    return invoice_links
    
def format_sheet(service, spreadsheet_id: str, last_row: int, sheet_id: int, last_row_cancellations: int | None = None) -> None:
    BLACK = {
        "red" : 0,
        "green" : 0,
        "blue" : 0,
        "alpha" : 1
    }
    
    WHITE = {
        "red" : 1,
        "green" : 1,
        "blue" : 1,
        "alpha" : 1
    }
    
    RED = {
        "red" : 230/255,
        "green" : 184/255,
        "blue" : 175/255,
        "alpha" : 1
    }
    
    GREEN = {
        "red" : 217/255,
        "green" : 234/255,
        "blue" : 211/255,
        "alpha" : 1
    }
    
    general_format = {
        "repeatCell" : {
            "range" : {
                "sheetId" : sheet_id,
                "endRowIndex" : last_row,
                "endColumnIndex" : 11
            },
            "cell" : {
                "userEnteredFormat" : {
                    "horizontalAlignment" : "CENTER",
                    "verticalAlignment" : "MIDDLE",
                    "wrapStrategy" : "CLIP",
                    "textFormat" : {
                        "fontFamily" : "Calibri",
                        "fontSize" : 11
                    }
                }
            },
            "fields" : "userEnteredFormat.horizontalAlignment, userEnteredFormat.verticalAlignment, userEnteredFormat.wrapStrategy, userEnteredFormat.textFormat.fontFamily, userEnteredFormat.textFormat.fontSize"
        }
    }
    
    merge_title = {
        "mergeCells" : {
            "range" : {
                "sheetId" : sheet_id,
                "endRowIndex" : 1,
                "endColumnIndex" : 11
            },
            "mergeType" : "MERGE_ROWS"
        }
    }
    
    headers_format = {
        "repeatCell" : {
            "range" : {
                "sheetId" : sheet_id,
                "endRowIndex" : 2,
                "endColumnIndex" : 11
            },
            "cell" : {
                "userEnteredFormat" : {
                    "backgroundColor" : BLACK,
                    "textFormat" : {
                        "foregroundColor" : WHITE,
                        "bold" : True
                    }
                }
            },
            "fields" : "userEnteredFormat.backgroundColor, userEnteredFormat.textFormat.foregroundColor, userEnteredFormat.textFormat.bold"
        }
    }
   
    checkboxes_range = {
        "sheetId" : sheet_id,
        "startRowIndex" : 2,
        "endRowIndex" : last_row,
        "startColumnIndex" : 1,
        "endColumnIndex" : 2
    }
    
    checkboxes = {
        "setDataValidation" : {
            "range" : checkboxes_range,
            "rule" : {
                "condition" : {
                    "type" : "BOOLEAN"
                },
                "showCustomUi" : True
            }
        }
    }
    
    checkboxes_format = {
        "repeatCell" : {
            "range" : checkboxes_range,
            "cell" : {
                "userEnteredFormat" : {
                    "textFormat" : {
                        "foregroundColor" : BLACK
                    }
                }
            },
            "fields" : "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    
    total_format = {
        "repeatCell" : {
            "range" : {
                "sheetId" : sheet_id,
                "startRowIndex" : 2,
                "endRowIndex" : last_row,
                "startColumnIndex" : 8,
                "endColumnIndex" : 9
            },
            "cell" : {
                "userEnteredFormat" : {
                    "numberFormat" : {
                        "type" : "NUMBER",
                        "pattern" : "#,##0.00"
                    }
                }
            },
            "fields" : "userEnteredFormat.numberFormat.type, userEnteredFormat.numberFormat.pattern"
        }
    }
    
    columns_width = [
        {
            "updateDimensionProperties" : {
                "properties" : {
                    "pixelSize" : width
                },
                "fields" : "pixelSize",
                "range" : {
                    "sheetId" : sheet_id,
                    "dimension" : "COLUMNS",
                    "startIndex" : idx,
                    "endIndex" : idx+1
                }
            }
        }
        for idx, width in enumerate([75, 35, 500, 35, 350, 35, 75, 75, 75, 50, 100])
    ]
    
    delete_conditional_formatting = {
        "requests" : [
            {
                "deleteConditionalFormatRule" : {
                    "index" : 0,
                    "sheetId" : sheet_id
                }
            }
            for _ in range(5)
        ]
    }
    
    rules = [
        {
            "value" : "=OR($C3=$C2; $C3=$C4)",
            "format" : {
                "textFormat" : {
                    "bold" : True
                }
            }
        },
        {
            "value" : "=$B3=TRUE",
            "format" : {
                "backgroundColor" : GREEN
            }
        },
        {
            "value" : "=AND($B3=TRUE; OR($C3=$C2; $C3=$C4))",
            "format" : {
                "backgroundColor" : GREEN,
                "textFormat" : {
                    "bold" : True
                }
            }
        },
        {
            "value" : "=REGEXMATCH($C3; \"CANCELADA\")",
            "format" : {
                "backgroundColor" : RED
            }
        }
    ]
    
    conditional_formatting = [
        {
            "addConditionalFormatRule" : {
                "rule" : {
                    "ranges" : [
                        {
                            "sheetId" : sheet_id,
                            "startRowIndex" : 2,
                            "endRowIndex" : last_row,
                            "endColumnIndex" : 11
                        }
                    ],
                    "booleanRule" : {
                        "condition" : {
                            "type" : "CUSTOM_FORMULA",
                            "values" : [
                                {
                                    "userEnteredValue" : rule["value"]
                                }
                            ]
                        },
                        "format" : rule["format"]
                    }
                },
                "index" : 0
            }
        }
        for rule in rules
    ]

    if last_row_cancellations:
        cancellations_general_format = {
            "repeatCell" : {
                "range" : {
                    "sheetId" : sheet_id,
                    "startRowIndex" : 1,
                    "endRowIndex" : last_row_cancellations,
                    "startColumnIndex" : 12,
                    "endColumnIndex" : 16
                },
                "cell" : {
                    "userEnteredFormat" : {
                        "horizontalAlignment" : "CENTER",
                        "verticalAlignment" : "MIDDLE",
                        "wrapStrategy" : "CLIP",
                        "textFormat" : {
                            "fontFamily" : "Calibri",
                            "fontSize" : 11
                        }
                    }
                },
                "fields" : "userEnteredFormat.horizontalAlignment, userEnteredFormat.verticalAlignment, userEnteredFormat.wrapStrategy, userEnteredFormat.textFormat.fontFamily, userEnteredFormat.textFormat.fontSize"
            }
        }
        
        cancellations_headers_format = {
            "repeatCell" : {
                "range" : {
                    "sheetId" : sheet_id,
                    "startRowIndex" : 1,
                    "endRowIndex" : 2,
                    "startColumnIndex" : 12,
                    "endColumnIndex" : 16
                },
                "cell" : {
                    "userEnteredFormat" : {
                        "backgroundColor" : BLACK,
                        "textFormat" : {
                            "foregroundColor" : WHITE,
                            "bold" : True
                        }
                    }
                },
                "fields" : "userEnteredFormat.backgroundColor, userEnteredFormat.textFormat.foregroundColor, userEnteredFormat.textFormat.bold"
            }
        }
        
        cancellations_checkboxes_range = {
            "sheetId" : sheet_id,
            "startRowIndex" : 2,
            "endRowIndex" : last_row_cancellations,
            "startColumnIndex" : 13,
            "endColumnIndex" : 14
        }
        
        cancellations_checkboxes = {
            "setDataValidation" : {
                "range" : cancellations_checkboxes_range,
                "rule" : {
                    "condition" : {
                        "type" : "BOOLEAN"
                    },
                    "showCustomUi" : True
                }
            }
        }
        
        
        cancellations_checkboxes_format = {
            "repeatCell" : {
                "range" : cancellations_checkboxes_range,
                "cell" : {
                    "userEnteredFormat" : {
                        "textFormat" : {
                            "foregroundColor" : BLACK
                        }
                    }
                },
                "fields" : "userEnteredFormat.textFormat.foregroundColor"
            }
        }
        
        cancellations_columns_width = [
            {
                "updateDimensionProperties" : {
                    "properties" : {
                        "pixelSize" : width
                    },
                    "fields" : "pixelSize",
                    "range" : {
                        "sheetId" : sheet_id,
                        "dimension" : "COLUMNS",
                        "startIndex" : idx+12,
                        "endIndex" : idx+13
                    }
                }
            }
            for idx, width in enumerate([75, 35, 500, 50])
        ]
        
        cancellations_conditional_formatting = {
            "addConditionalFormatRule" : {
                "rule" : {
                    "ranges" : [
                        {
                            "sheetId" : sheet_id,
                            "startRowIndex" : 2,
                            "endRowIndex" : last_row_cancellations,
                            "startColumnIndex" : 12,
                            "endColumnIndex" : 16
                        }
                    ],
                    "booleanRule" : {
                        "condition" : {
                            "type" : "CUSTOM_FORMULA",
                            "values" : [
                                {
                                    "userEnteredValue" : "=OR($O3=$O2; $O3=$O4)"
                                }
                            ]
                        },
                        "format" : {
                            "textFormat" : {
                                "bold" : True
                            }
                        }
                    }
                },
                "index" : 0
            }
        }

    body = {
        "requests" : []
    }
    
    for request in [general_format, merge_title, headers_format, checkboxes, checkboxes_format, total_format]:
        body["requests"].append(request)
    
    if last_row_cancellations:
        for request in [cancellations_general_format, cancellations_headers_format, cancellations_checkboxes, cancellations_checkboxes_format, cancellations_columns_width, cancellations_conditional_formatting]:
            body["requests"].append(request)
    
    body["requests"].extend(columns_width)
    body["requests"].extend(conditional_formatting)
    
    try:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                           body=delete_conditional_formatting).execute()
    except HttpError:
        pass
    
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                       body=body).execute()

def clear_cancellations_range(service, spreadsheet_id: str, sheet_name: str, sheet_id: int, last_row: int) -> None:
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,
                                          range=f"'{sheet_name}'!M2:P{last_row}").execute()
    
    body = {
        "requests" : [
            {
                "updateDimensionProperties" : {
                    "properties" : {
                        "pixelSize" : 100
                    },
                    "fields" : "pixelSize",
                    "range" : {
                        "sheetId" : sheet_id,
                        "dimension" : "COLUMNS",
                        "startIndex" : 12,
                        "endIndex" : 16
                    }
                }
            },
            {
                "repeatCell" : {
                    "range" : {
                        "sheetId" : sheet_id,
                        "startRowIndex" : 1,
                        "endRowIndex" : last_row,
                        "startColumnIndex" : 12,
                        "endColumnIndex" : 16
                    },
                    "fields" : "*"
                }
            }
        ]
    }
    
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                       body=body).execute()