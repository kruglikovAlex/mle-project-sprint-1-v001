#from airflow.plugins_manager import AirflowPlugin
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(token='7825435107:AAHmLbRhQM1DUWhOvnVvhSjNBuSH63TvYVY', chat_id='-4775314654')
    dag = context['dag'].dag_id
    run_id = context['run_id']

    message = f'Исполнение DAG {dag} c id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': '-4775314654',
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(token='7825435107:AAHmLbRhQM1DUWhOvnVvhSjNBuSH63TvYVY', chat_id='-4775314654')
    dag = context['dag'].dag_id
    run_id = context['run_id']

    message = f'Исполнение DAG {dag} c id={run_id} окончилось неудачей!'
    hook.send_message({
        'chat_id': '-4775314654',
        'text': message
    })

#class MyFirstPlugin(AirflowPlugin):
#    name = 'my_first_plugin'
#    operators = [send_telegram_failure_message, send_telegram_success_message]