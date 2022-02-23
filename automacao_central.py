from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import json
import logging
import requests
import os
import sys


CONFIG_PATH = 'config.json'


def load_configuration():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        text = f.read()
        return json.loads(text)
    

conf = load_configuration()


def save_configuration():
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        text = json.dumps(conf, indent=4, ensure_ascii=False)
        f.write(text)


def connect_to_database():
    cnx = mysql.connector.connect(host='seguralta.cfit69fwielr.us-east-1.rds.amazonaws.com',
    user='seguser', password='6KRWYsa3rkOP0pHJjLJ1', database='centralseguralta')
    return cnx


def read_last_execution_time():
    return datetime.fromisoformat(conf['last_execution'])


def write_last_execution_time(dt):
    conf['last_execution'] = dt.isoformat()
    save_configuration()


def pick_user():
    users = conf['users']
    ui = conf['last_user_index']
    user = users[ui]
    conf['last_user_index'] = (ui + 1) % len(users)
    save_configuration()
    return user


def create_lead(cursor, row):
    lead = {}

    for i in range(len(cursor.column_names)):
        cn = cursor.column_names[i]
        lead[cn] = row[i]
    
    owner_name = lead.pop('name', '')
    owner_email  = lead.pop('email', '')

    if owner_name == '':
        owner = pick_user()
        owner_name = owner['name']
        owner_email = owner['email']

    lead['owner_name'] = owner_name
    lead['owner_email'] = owner_email

    lead['name'] = lead.pop('consumer_name')
    lead['email'] = lead.pop('consumer_email')
    phones = lead.pop('phones')

    try:
        lead['phone'] = json.loads(phones)['primary']
    except:
        lead['phone'] = ''

    lead['type'] = str(lead['type'])
    return lead


def get_new_leads():
    cnx = connect_to_database()
    cursor = cnx.cursor()
    start_dt = read_last_execution_time()
    end_dt = datetime.now()
    logging.info('Buscando leads comprados desde {} até {}'.format(start_dt, end_dt))
    write_last_execution_time(end_dt)
    query = ("""
    select l.id, l.consumer_name, l.consumer_email, l.phones, u.name, u.email, l.type, l.created_at, l.paid_at from balances b
    join users u
    on b.user = u.id 
    join leads l
    on l.id = b.auth_id
    where b.company = 1448
    and b.created_at between '{}' and '{}'
    and description like "Compra de Lead%"
    and (l.refund is null or l.refund = 0)
    order by b.created_at asc;
    """.format(start_dt, end_dt))
    cursor.execute(query)
    leads = []

    for row in cursor:
        lead = create_lead(cursor, row)
        leads.append(lead)
    
    expired_leads_query = """
    SELECT l.id, l.consumer_name, l.consumer_email, l.phones, l.type, l.created_at, l.paid_at FROM leads l
    WHERE l.expired = 1
    AND l.company = 1448
    AND l.paid_at BETWEEN '{}' AND '{}'
    AND (l.refund IS NULL or l.refund = 0)
    ORDER BY paid_at ASC
    """.format(start_dt, end_dt)
    cursor.execute(expired_leads_query)
    expired_leads = []

    for row in cursor:
        lead = create_lead(cursor, row)
        expired_leads.append(lead)

    if len(expired_leads) > 0:
        logging.info('%d leads expirados encontrados', len(expired_leads))
        leads = leads + expired_leads

    for lead in leads:
        query = '''
        select li.param, li.value from leads_infos li
        where li.lead = {}
        '''.format(lead['id'])
        cursor.execute(query)
        info = ""
        for (param, value) in cursor:
            info += "{}: {}\n".format(param, value)
        info = info.strip()
        lead['info'] = info

    cursor.close()
    cnx.close()
    logging.info('{} leads encontrados'.format(len(leads)))
    
    return leads


def send_lead(lead):
    api_token = conf['crm']['token']

    if lead['owner_email'] not in conf['crm']['user_ids']:
        return

    if conf['debug']: # enviar dados para a conta dev no modo de debug
        lead['owner_email'] = 'dev@seguralta.com.br'
        lead['owner_name'] = 'dev'

    url = 'https://plugcrm.net/api/v1/deals?token={}'.format(api_token)
    owner_id = conf['crm']['user_ids'][lead['owner_email']]
    body = {
        'deal': {
            'name': lead['name'],
            'user_id': owner_id,
            'deal_stage_id': conf['crm']['deal_stage_id']
        },
        'contacts': [
            {
                'name': lead['name'],
                'emails': [
                    {
                        'email': lead['email']
                    }
                ],
                'phones': [
                    {
                        'phone': lead['phone']
                    }
                ]
            }
        ],
        'organization': {
            'name': lead['name']
        },
    }
    # Coloca o tipo do lead em um campo customizado
    if lead['type'] in conf['lead_types']:
        body['deal']['deal_custom_fields'] = [
            {
                'custom_field_id': conf['crm']['cf_lead_type_id'],
                'value': conf['lead_types'][lead['type']]
            }
        ]
    res = requests.post(url, json=body)
    res.raise_for_status()
    ro = json.loads(res.text)
    logging.info(
'''Lead enviado para o RD Station CRM
    Central de Leads
        ID: %d
        Nome: %s
        Email: %s
        Criado: %s
        Comprado: %s
    RD Station CRM
        ID: %s
        Responsavel: %s
        Email Responsavel: %s''',
        lead['id'], lead['name'], lead['email'], lead['created_at'], lead['paid_at'],
        ro['user']['id'], ro['user']['name'], ro['user'].get('email', '-'))
    # Envio das informacoes adicionais como anotacao no RD Station
    ro = json.loads(res.text)
    url = 'https://plugcrm.net/api/v1/activities?token={}'.format(conf['crm']['token'])
    body = {
        'activity': {
            'user_id': owner_id,
            'deal_id': ro['_id'],
            'text': lead['info']
        }
    }
    res = requests.post(url, json=body)


def send_leads(leads):
    for lead in leads:
        send_lead(lead)


def handle_exception(type, value, traceback):
    logging.critical("Uncaught exception", exc_info=(type, value, traceback))


if __name__ == "__main__":
    log_dir = conf['log_directory']

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    log_file = '{}/{}.log'.format(log_dir, datetime.now().strftime('%Y-%m-%d'))
    log_level = logging.INFO
    
    if conf['debug']:
        log_level = logging.DEBUG  

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        encoding='utf-8',
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    sys.excepthook = handle_exception # faz com que erros de execucao sejam logados
    logging.info('INÍCIO DA EXECUÇÃO')
    leads = get_new_leads()
    send_leads(leads)