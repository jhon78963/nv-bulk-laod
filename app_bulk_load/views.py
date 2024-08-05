from .models import *
from rest_framework.decorators import api_view
from django.http import JsonResponse
from rest_framework import status
import pandas as pd
from django.db import connection
from django.db import transaction
import psycopg2
import psycopg2.extras

@api_view(['POST'])
def all_vouchers_upload_excel(request):
    try:
        if request.method == 'POST' and 'file' in request.FILES:
            excel_file = request.FILES['file']
            required_columns = [
                'GRADO', 'ALUMNO', 'DESCRIPCION', 'RECIBO', 'IMPORTE', 'FECHA', "N.OP"
            ]

            try:
                excel = pd.read_excel(excel_file)
            except Exception as error:
                return JsonResponse({
                    'message': 'Error al leer el archivo Excel',
                    'details': str(error)
                }, status=status.HTTP_400_BAD_REQUEST)
            
            missing_columns = [col for col in required_columns if col not in excel.columns]
            if missing_columns:
                missing_columns_str = ', '.join(missing_columns)
                return JsonResponse({
                    'message': f'Falta columna(s): {missing_columns_str}',
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Transformar los datos
            df_excel, df_insert_load, state_error = transformDataAllVouchers(excel)

            if state_error:
                return JsonResponse({
                    'message': 'Hay errores en el excel importado, revisar y volver a subir',
                    'data': df_excel.to_dict('records')
                }, status=status.HTTP_400_BAD_REQUEST)

            if len(df_insert_load) <= 0:
                return JsonResponse({
                    'message': 'Sin data para insertar',
                    'data': df_excel.to_dict('records')
                }, status=status.HTTP_400_BAD_REQUEST)

            itFailed, error_message = LoadDataAllVoucher(df_insert_load)
            if itFailed:
                return JsonResponse({
                    'message': f'Error al insertar data: {error_message}',
                    'data': []
                }, status=status.HTTP_400_BAD_REQUEST)

            return JsonResponse({
                'message': 'Ejecución satisfactoria',
                'data': df_excel.to_dict('records')
            }, status=status.HTTP_200_OK)

    except Exception as error:
        return JsonResponse({
            'message': 'Ejecución fallida',
            'details': str(error)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    return JsonResponse({
        'error': 'Método no permitido'
    }, status=status.HTTP_405_METHOD_NOT_ALLOWED)

def transformDataAllVouchers(excel):
    df_excel = pd.DataFrame()
    df_excel_ = pd.DataFrame()
    df_load = pd.DataFrame()
    state_error = False
    try:
        df_excel = excel.rename(columns={
            'GRADO': 'grade', 
            'ALUMNO': 'student', 
            'DESCRIPCION': 'description', 
            'RECIBO': 'voucher', 
            'IMPORTE': 'amount',
            "FECHA": "date",
            "N.OP": "no_operation"
        })

        df_excel['voucher'] = df_excel['voucher'].astype(str)
        df_excel['amount'] = df_excel['amount'].astype(str)

        df_excel = df_excel.fillna({
            'grade': '',
            'student': '',
            'description': '',
            'voucher': '',
            'amount': '',
            'date': '',
            'no_operation': ''
        })

        df_excel_ = df_excel.copy()  # Ensure df_excel_ is a copy of df_excel
        df_excel_["errors"] = df_excel_.apply(calcErroresAllVouchers, axis=1)
        df_excel_["state"] = df_excel_["errors"].apply(lambda x: 1 if len(str(x).strip()) == 0 else 0)

        # Debugging line
        # print("Errors DataFrame:", df_excel_["errors"].tolist())  

        state_error = True if (df_excel_["state"].sum() != len(df_excel_)) else False

        # Verificar datos duplicados
        existing_data = check_existing_data(df_excel_)
        if existing_data:
            return df_excel_, pd.DataFrame(), True
        
        df_load = df_excel_.loc[df_excel_["state"] == 1, ['grade', 'student', 'description', 'voucher', 'amount', 'date', 'no_operation']]
    except Exception as error:
        # Debugging line
        # print("Exception in transformDataAllVouchers:", error)
        return pd.DataFrame(), pd.DataFrame(), True  # Return three values in case of an error
    
    return df_excel_, df_load, state_error
def check_existing_data(df):
    voucher_list = df['voucher'].tolist()
    if not voucher_list:
        return False
    existing_voucher_query = "SELECT voucher FROM bulk_load_all_vouchers WHERE voucher IN %s"
    cursor = connection.cursor()
    cursor.execute(existing_voucher_query, [tuple(voucher_list)])
    existing_vouchers = cursor.fetchall()
    return len(existing_vouchers) > 0

def LoadDataAllVoucher(dataFrame):
    itFailed = True
    error_message = ""
    try:
        # Prepare SQL Query
        columnsLst = ['grade', 'student', 'description', 'voucher', 'amount', 'date', 'no_operation']
        columns = ",".join(columnsLst)
        sqlValuesStatement = "VALUES({})".format(",".join(["%s" for _ in columnsLst]))
        insertQuery = "INSERT INTO bulk_load_all_vouchers ({}) {}".format(columns, sqlValuesStatement)

        cursor = connection.cursor()
        # Inicio de Transacción
        with transaction.atomic():
            # Debugging: Check the data being inserted
            # print("SQL Query:", insertQuery)
            # print("DataFrame values:", dataFrame.values)
            
            # Insert Query
            psycopg2.extras.execute_batch(cursor, insertQuery, dataFrame.values.tolist())
            itFailed = False
    except Exception as error:
        connection.rollback()
        error_message = str(error)
        # print("Error:", error)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return itFailed, error_message

def calcErroresAllVouchers(row):
    errors = []
    
    # if row['grade'] == '':
    #     errors.append("Grado vacío")

    # if row['student'] == '':
    #     errors.append("Estudiante vacío")

    # if row['description'] == '':
    #     errors.append("Descripción vacío")

    # if row['voucher'] == '':
    #     errors.append("Voucher vacío")

    # if row['amount'] == '':
    #     errors.append("Monto vacío")

    # if row['date'] == '':
    #     errors.append("Fecha vacía")

    # if row['no_operation'] == '':
    #     errors.append("Nro de operación vacío")

    return ", ".join(errors)