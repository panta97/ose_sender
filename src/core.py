import os
import datetime
import boto3
from src.enums import MONTH_SPANISH
from src.connection import get_ncs, execute_sql

def get_pending_cpes():
    query = """
    select distinct fecha
    from cpe_aux
    order by fecha desc
    limit 1;
    """
    pending_dates = []
    last_sent_date = datetime.datetime.combine(get_ncs(query)[0][0], datetime.datetime.min.time())
    last_sent_date += datetime.timedelta(days=1)
    today_date = datetime.datetime.now()
    today_date -= datetime.timedelta(days=1) # ALWAYS ONE DAY BEFORE
    today_date -= datetime.timedelta(hours=3) # CRONJOB GETS EXECUTED AT 3 AM GTM-5

    while last_sent_date < today_date:
        pending_dates.append(last_sent_date.strftime('%Y-%m-%d'))
        last_sent_date += datetime.timedelta(days=1)

    return pending_dates

def get_cpe(date):
    s3 = boto3.client('s3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"))
    bucket = os.getenv("AWS_BUCKET")

    result_obj = {
        'status': '',
        'content': '',
    }

    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
    except Exception as e:
        result_obj['status'] = 'ERROR'
        result_obj['content'] = 'BAD DATE: {}'.format(e.args)
        return result_obj

    key = 'CPEs/{year}/{month}/{sqlfile}'.format(
        year=date_obj.year,month=MONTH_SPANISH[date_obj.month],sqlfile='cpe_{}.sql'.format(date)
    )

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        result_obj['status'] = 'ERROR'
        result_obj['content'] = 'COULD NOT DOWNLOAD FROM S3: {}'.format(e.args)
        return result_obj

    result_obj['status'] = 'SUCCESS'
    result_obj['content'] = {
        'filename' : 'cpe_{}.sql'.format(date),
        'content' : response['Body'].read().decode('utf-8').replace('INSERT INTO ose.cpe', 'INSERT INTO ose.cpe_aux', 1)
    }
    return result_obj


def cpes_check(type):
    all_sent_query = """
    select distinct enviado
    from cpe_aux;
    """
    all_sent = get_ncs(all_sent_query)
    # CLEAN RESULT
    all_sent = list(map(lambda x: x[0], all_sent))
    return type in all_sent


def download_cpe(manual_dates=None):
    # CHECK IF THERE ARE CPES LEFT TO BE SEND (NULL)
    # IN ORDER TO DOWNLOAD THE CPES
    if cpes_check(None):
        raise Exception('THERE ARE CPES LEFT TO BE SEND (NULL)')

    if manual_dates is None:
        pendings = get_pending_cpes()
    else:
        pendings = manual_dates
    for pending_date in pendings:
        result_obj = get_cpe(pending_date)
        if result_obj['status'] == 'SUCCESS':
            cpe = result_obj['content']
            execute_sql(cpe['content'])

    if len(pendings) > 0:
        result = "DOWNLOADED DATES: {} FROM S3".format(
            ", ".join(pendings)
        )
    else:
        result = "THERE ARE NO CPES LEFT TO DOWNLOAD"
    return result


def get_pending_dates():
    dates_query = """
    select distinct fecha
    from cpe_aux
    where enviado = 'pending'
    order by fecha desc;
    """
    pending_dates = get_ncs(dates_query)
    pending_dates = list(map(lambda x: x[0], pending_dates))
    return pending_dates

def send_cpe():
    # CHECK IF THERE ARE CPES TO BE CONFIRMED (PENDING)
    if cpes_check('pending'):
        raise Exception('THERE ARE CPES TO BE CONFIRMED (PENDING)')

    # A BUNDLE HAS A MAX CPES OF 500 ANYTHING MORE THAN THAT
    # WILL PRODUCE ERRORS
    upload_query = """
    CREATE TEMPORARY TABLE to_upload AS (
        select id
        from cpe_aux c
                right join (
            select distinct serie serie, numero numero, fecha fecha
            from cpe_aux
            where enviado is null
            order by fecha
            limit {max}
        ) t
        on c.serie = t.serie
            and c.numero = t.numero
    );""".format(max=os.getenv("MAX_CPE_BUNDLE_SIZE"))
    execute_sql(upload_query)

    upload_query = """
    update cpe_aux
        set enviado = 'pending'
    where id in (
        select id from to_upload
        );"""
    execute_sql(upload_query)

    upload_query = """
    insert into cpe (tipo_cpe, serie, numero, fecha, codigo_cliente, tipo_documento, documento, razon_social, direccion, email, sub_total, descuento_global, igv, total, tipo_cpe_nc, serie_nc, numero_nc, fecha_nc, codigo_articulo, descripcion, unidad_medida, cantidad, precio_unitario, sub_total_art, igv_art, total_art)
    select tipo_cpe, serie, numero, fecha, codigo_cliente, tipo_documento, documento, razon_social, direccion, email, sub_total, descuento_global, igv, total, tipo_cpe_nc, serie_nc, numero_nc, fecha_nc, codigo_articulo, descripcion, unidad_medida, cantidad, precio_unitario, sub_total_art, igv_art, total_art
    from cpe_aux
    where enviado = 'pending'; """
    execute_sql(upload_query)

    upload_query = """
    drop table to_upload;
    """
    execute_sql(upload_query)

    # CREATE RESULT
    pending_dates = get_pending_dates()
    result = "SENT CPES DATES FROM '{}' TO '{}' INTO CPE TABLE" \
        .format(pending_dates[0], pending_dates[len(pending_dates) - 1])
    return result

def confirm_cpe():
    # GET PENDING CPES BEFORE UPDATING THEM
    pending_dates = get_pending_dates()

    confirm_query = """
    update cpe_aux set enviado = 'yes'
    where enviado = 'pending';
    """
    execute_sql(confirm_query)

    result = "UPDATED CPES DATES FROM '{}' TO '{}'" \
        .format(pending_dates[0], pending_dates[len(pending_dates) - 1])

    cpes_null_query = """
    select count(1)
    from cpe_aux
    where enviado is NULL;
    """
    cpes_null_count = get_ncs(cpes_null_query)[0][0]

    if cpes_null_count > 0:
        result += '\n'
        result += 'THERE ARE STILL {} YET TO BE SENT...' \
            .format(cpes_null_count)

    return result
