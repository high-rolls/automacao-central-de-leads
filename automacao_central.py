import argparse
import dotenv
import json
import logging
import mysql.connector
import os
import pytz
import requests
import sys
from datetime import datetime
from mysql.connector import errorcode
from urllib.error import HTTPError

CONFIG_PATH = 'config.json'

cdl_paid_leads_query = """
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
"""

cdl_free_leads_query = """
SELECT l.id, l.consumer_name, l.consumer_email, l.phones, l.type, l.created_at, l.paid_at
FROM leads l
WHERE l.status in('new', 'waiting_proposal')
AND l.company = 1448
AND (l.paid_price IS NULL OR l.paid_price = 0)
AND (l.refund IS NULL OR l.refund = 0)
AND l.paid_at BETWEEN '{}' AND ''
ORDER BY l.paid_at DESC;
"""

my_leads_query = """
SELECT l.id, l.consumer_name, l.consumer_email, l.phones, c.name, c.email, l.type, l.created_at, l.paid_at
FROM leads l
JOIN companies c 
ON l.company = c.company
WHERE l.status in('new', 'waiting_proposal')
AND c.company != 1448
AND l.paid_at BETWEEN '{}' AND '{}'
ORDER BY l.paid_at DESC;
"""


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
    le = os.getenv('AUTOMACAO_CENTRAL_LAST_EXECUTION')
    if le:
        return datetime.fromisoformat(le)
    else:
        return None


def write_last_execution_time(dt):
    dotenv.set_key('.env', 'AUTOMACAO_CENTRAL_LAST_EXECUTION', dt.isoformat())


def pick_user():
    users = conf['users']
    ui = conf['last_user_index']
    user = users[ui]
    conf['last_user_index'] = (ui + 1) % len(users)
    save_configuration()
    return user


def load_RDS_users():
    url = f"https://plugcrm.net/api/v1/users?token={conf['crm']['token']}"
    response = requests.get(url)
    response.raise_for_status()
    obj = json.loads(response.text)
    users = obj['users']
    return users


def update_users():
    users = load_RDS_users()
    conf['crm']['user_ids'].clear()
    for user in users:
        email = user['email']
        conf['crm']['user_ids'][email] = user['id']
    save_configuration()


def create_lead(cursor, row):
    lead = {}

    for i in range(len(cursor.column_names)):
        column_name = cursor.column_names[i]
        lead[column_name] = row[i]
    
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


def db_load_leads(cursor, query):
    leads = []
    cursor.execute(query)
    for row in cursor:
        lead = create_lead(cursor, row)
        if lead['owner_email'] in conf['crm']['user_ids']:
            leads.append(lead)
    return leads


def get_new_leads():
    cnx = connect_to_database()
    cursor = cnx.cursor()
    db_timezone = pytz.timezone("America/Sao_Paulo")
    current_dt = datetime.now().astimezone(db_timezone)
    start_dt = read_last_execution_time()
    if not start_dt:
        write_last_execution_time(current_dt)
        logging.info("Data inicial de busca de leads configurada no sistema, encerrando...")
        exit(0)
    start_dt = start_dt.astimezone(db_timezone)
    end_dt = current_dt
    logging.info('Buscando leads comprados desde {} até {}'.format(start_dt, end_dt))
    write_last_execution_time(end_dt)
    cdl_paid_leads = db_load_leads(cursor, cdl_paid_leads_query.format(start_dt, end_dt))
    cdl_free_leads = db_load_leads(cursor, cdl_free_leads_query.format(start_dt, end_dt))
    my_leads = db_load_leads(cursor, my_leads_query.format(start_dt, end_dt))
    leads = cdl_paid_leads + cdl_free_leads + my_leads

    for lead in leads:
        query = '''
        select li.param, li.value from leads_infos li
        where li.lead = {}
        '''.format(lead['id'])
        cursor.execute(query)
        info = {} # dicionario de dados contendo as informacoes adicionais do lead
        for (param, value) in cursor:
            info[param] = value
        lead['info'] = info

    cursor.close()
    cnx.close()
    if leads:
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
    custom_fields = []

    if lead['type'] in conf['lead_types']:
        custom_fields.append(
            {
                'custom_field_id': conf['crm']['cf_lead_type_id'],
                'value': conf['lead_types'][lead['type']]
            }
        )
    
    for param in lead['info']:
        if param in conf['crm']['custom_field_ids']:
            custom_fields.append(
                {
                    'custom_field_id': conf['crm']['custom_field_ids'][param],
                    'value': lead['info'][param]
                }
            )

    
    body['deal']['deal_custom_fields'] = custom_fields
    res = requests.post(url, json=body)
    try:
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
    except HTTPError:
        logging.error("Erro ao enviar lead %s", lead[id])
        ro = json.loads(res.text)
        logging.error(json.dumps(ro, indent=4, ensure_ascii=False))


def send_leads(leads):
    for lead in leads:
        send_lead(lead)


def handle_exception(type, value, traceback):
    logging.critical("Uncaught exception", exc_info=(type, value, traceback))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script de integracao da Central de Leads com o RD Station"
    )
    parser.add_argument("-u", "--update-users",
        help="Atualiza a lista de usuarios do arquivo de configuracao",
        action="store_true")
    args = parser.parse_args()

    log_dir = conf['log_directory']

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    log_file = '{}/{}.log'.format(log_dir, datetime.now().strftime('%Y-%m-%d'))
    log_level = logging.INFO
    
    if conf['debug']:
        log_level = logging.DEBUG  

    handler = logging.FileHandler(log_file, encoding='utf-8')
    logging.basicConfig(
        handlers=[handler],
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    sys.excepthook = handle_exception # faz com que erros de execucao sejam logados
    if args.update_users:
        update_users()
        sys.exit()
    
    dotenv.load_dotenv()
    
    leads = get_new_leads()
    send_leads(leads)
